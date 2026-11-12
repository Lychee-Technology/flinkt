package io.flinkt.registry

import io.flinkt.FlinkType
import io.flinkt.codec.CodecFactory
import io.flinkt.codec.GeneratedCodec
import io.flinkt.schema.DeclarationSchema
import io.flinkt.types.BuiltInKind
import io.flinkt.types.FlinktType
import io.flinkt.types.TypeShape
import java.util.Collections
import java.util.ServiceConfigurationError
import java.util.ServiceLoader
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * The generated types visible from one class loader, and the two steps from a Kotlin type to its codec:
 *
 * - [model] analyzes a `KType` into Flinkt's type model, for every type Kotlin can express;
 * - [codec] resolves a model to a codec for the shapes this release supports, and fails with
 *   `UnsupportedTypeException` for every other.
 *
 * A registry belongs to the class loader it was [loaded][load] from. Flink loads each job's code in a class loader of
 * its own and can run several jobs in one JVM, so generated types are identified by their `Class` objects, never by
 * name: two classes with the same name from two class loaders are two types, and a registry finds only its own.
 * Nothing here is cached across registries or kept in a static field, so a registry, and the class loader it holds
 * classes of, can be collected once its job no longer uses it.
 *
 * A registry is immutable once built and can be shared between threads.
 */
public class TypeRegistry private constructor(
    /** The modules the registry was built from, in the order they registered. */
    public val modules: List<GeneratedTypeModule>,
    private val generated: Map<Class<*>, GeneratedDeclaration<*>>,
    private val builtIns: Map<BuiltInKind, BuiltInRegistration>,
) {
    private val analyzer = TypeAnalyzer { type -> generated[type]?.let { TypeShape.Generated(it.schema) } }

    /** Every generated declaration the modules registered, in the order they registered. */
    public val declarations: List<GeneratedDeclaration<*>> = Collections.unmodifiableList(generated.values.toList())

    /** The declaration registered for [type], or `null`. A class of the same name from another class loader isn't [type]. */
    public fun declaration(type: Class<*>): GeneratedDeclaration<*>? = generated[type]

    /** The declaration registered for [type], or `null`. A class of the same name from another class loader isn't [type]. */
    public fun declaration(type: KClass<*>): GeneratedDeclaration<*>? = generated[type.java]

    /**
     * Structural analysis: Flinkt's model of [type], keeping its classifier, every nested type argument with its
     * variance, and nullability at every level. A generated declaration's shape comes from its registered schema.
     *
     * This doesn't check that Flinkt supports the type. A platform type from Java is modeled as non-null, and a Java
     * collection as Kotlin's read-only collection.
     */
    public fun model(type: KType): FlinktType = analyzer.analyze(type)

    /**
     * Supported resolution: the codec of [type].
     *
     * This release resolves the built-in types whose codecs a module registers, and non-null, non-generic generated
     * records whose fields it can resolve. Every other type is modeled but fails with `UnsupportedTypeException`,
     * whose message names the canonical type and the reason.
     *
     * [type] may come from another registry. It's resolved as this registry models the same classes, arguments and
     * nullability: each generated declaration, at every depth, is this registry's own declaration of that class,
     * schema and factory alike, and a class this registry has no declaration for is known by its classifier only.
     * The shape another registry gave a class doesn't decide anything here.
     */
    public fun codec(type: FlinktType): GeneratedCodec<*> = Resolution(this).resolve(reconcile(type))

    /** Supported resolution of a `KType`: [codec] of its [model]. */
    public fun codec(type: KType): GeneratedCodec<*> = Resolution(this).resolve(model(type))

    /** [type], which any registry may have built, as this registry models it. */
    internal fun reconcile(type: FlinktType): FlinktType = analyzer.reconcile(type)

    internal fun builtIn(kind: BuiltInKind): BuiltInRegistration? = builtIns[kind]

    /** Why [type], a class without a codec in this registry, isn't resolved. */
    internal fun unmodeledReason(type: FlinktType): String {
        val jvmClass = checkNotNull(type.jvmClass)
        val sameName = generated.keys.firstOrNull { it.name == jvmClass.name && it != jvmClass }
        return when {
            sameName != null ->
                "this registry has a class of that name from another class loader (${sameName.classLoader}), " +
                    "but this one comes from ${jvmClass.classLoader}. Resolve a type with the registry of its own class loader"
            jvmClass.isAnnotationPresent(FlinkType::class.java) ->
                "it's annotated with @FlinkType, but no GeneratedTypeModule that this registry found registers it. " +
                    "Check that flinkt-ksp runs in the module that declares it"
            else -> "it isn't a built-in type, and it isn't annotated with @FlinkType"
        }
    }

    public companion object {
        /**
         * Builds the registry of [classLoader], from the [GeneratedTypeModule]s that `java.util.ServiceLoader` finds
         * through it. Each call builds a new registry and keeps nothing.
         *
         * Fails if a module can't be loaded or fails to register, or if a type is registered twice.
         */
        public fun load(classLoader: ClassLoader): TypeRegistry {
            val modules = try {
                ServiceLoader.load(GeneratedTypeModule::class.java, classLoader).toList()
            } catch (e: ServiceConfigurationError) {
                throw IllegalStateException("A GeneratedTypeModule couldn't be loaded through $classLoader: ${e.message}", e)
            }
            return of(modules)
        }

        /** Builds a registry from [modules] found some other way, such as in a test. */
        public fun of(modules: Iterable<GeneratedTypeModule>): TypeRegistry {
            val builder = Builder()
            val list = modules.toList()
            list.forEach(builder::add)
            return TypeRegistry(Collections.unmodifiableList(list), builder.generated, builder.builtIns)
        }

        /** Builds a registry from [modules] found some other way, such as in a test. */
        public fun of(vararg modules: GeneratedTypeModule): TypeRegistry = of(modules.asList())
    }

    private class Builder {
        val generated = LinkedHashMap<Class<*>, GeneratedDeclaration<*>>()
        val builtIns = LinkedHashMap<BuiltInKind, BuiltInRegistration>()

        fun add(module: GeneratedTypeModule) {
            val scope = Scope(module)
            try {
                module.register(scope)
            } catch (e: RegistrationException) {
                throw e
            } catch (e: RuntimeException) {
                throw IllegalStateException("${describe(module)} failed to register its types: ${e.message}", e)
            } finally {
                scope.open = false
            }
        }

        private inner class Scope(private val module: GeneratedTypeModule) : MutableTypeRegistry {
            var open = true

            override fun <T : Any> register(type: KClass<T>, schema: DeclarationSchema, codecFactory: CodecFactory<T>) {
                checkOpen()
                val jvmClass = type.java
                if (schema.typeName != jvmClass.name) {
                    throw RegistrationException(
                        "${describe(module)} registers ${jvmClass.name} with the schema of ${schema.typeName}",
                    )
                }
                if (jvmClass.isPrimitive || BuiltInKind.entries.any { it.jvmClass == jvmClass }) {
                    throw RegistrationException(
                        "${describe(module)} registers the built-in type ${jvmClass.name} as a generated declaration. " +
                            "A built-in type's codec is registered with registerBuiltIn",
                    )
                }
                generated[jvmClass]?.let { existing ->
                    throw RegistrationException(twice(jvmClass.name, existing.module, module))
                }
                generated[jvmClass] = GeneratedDeclaration(type, schema, codecFactory, module)
            }

            override fun <T : Any> registerBuiltIn(type: KClass<T>, codecFactory: CodecFactory<T>) {
                checkOpen()
                val kind = BuiltInKind.entries.firstOrNull { it.jvmClass == type.javaObjectType }
                    ?: throw RegistrationException(
                        "${describe(module)} registers a built-in codec for ${type.javaObjectType.name}, which isn't a " +
                            "built-in type. The built-in types are ${BuiltInKind.entries.joinToString { it.kotlinName }}",
                    )
                builtIns[kind]?.let { existing ->
                    throw RegistrationException(twice("the built-in type ${kind.kotlinName}", existing.module, module))
                }
                builtIns[kind] = BuiltInRegistration(codecFactory, module)
            }

            private fun checkOpen() {
                if (!open) {
                    throw RegistrationException(
                        "${describe(module)} registered a type after its register() returned. " +
                            "A MutableTypeRegistry is only valid while register() runs",
                    )
                }
            }
        }
    }
}

/** Analyzes [T] with [TypeRegistry.model]. */
public inline fun <reified T> TypeRegistry.model(): FlinktType = model(typeOf<T>())

/** Resolves [T] with [TypeRegistry.codec]. */
@Suppress("UNCHECKED_CAST")
public inline fun <reified T> TypeRegistry.codec(): GeneratedCodec<T> = codec(typeOf<T>()) as GeneratedCodec<T>

/**
 * A generated declaration as a [GeneratedTypeModule] registered it: its class, its schema, and its codec factory.
 */
public class GeneratedDeclaration<T : Any> internal constructor(
    /** The declaration's class, which is its identity in the registry. */
    public val type: KClass<T>,
    public val schema: DeclarationSchema,
    public val codecFactory: CodecFactory<T>,
    /** The module that registered it. */
    public val module: GeneratedTypeModule,
) {
    override fun toString(): String = "${type.java.name}, registered by ${describe(module)}"
}

internal class BuiltInRegistration(val codecFactory: CodecFactory<*>, val module: GeneratedTypeModule)

/** A registration error. It already names the module, so registering doesn't wrap it. */
private class RegistrationException(message: String) : IllegalStateException(message)

private fun twice(what: String, first: GeneratedTypeModule, second: GeneratedTypeModule): String =
    if (first === second) {
        "$what is registered twice by ${describe(first)}"
    } else {
        "$what is registered twice: by ${describe(first)} and by ${describe(second)}"
    }

/** A module's class name, and where the class was loaded from when the JVM says. */
internal fun describe(module: GeneratedTypeModule): String {
    val type = module.javaClass
    val location = try {
        type.protectionDomain?.codeSource?.location
    } catch (e: SecurityException) {
        null
    }
    return if (location == null) type.name else "${type.name} ($location)"
}
