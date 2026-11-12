package io.flinkt.registry

import io.flinkt.codec.CodecFactory
import io.flinkt.schema.DeclarationSchema
import io.flinkt.types.BuiltInKind
import kotlin.reflect.KClass

/**
 * The generated types of one compilation.
 *
 * KSP generates one implementation per compilation and lists it in
 * `META-INF/services/io.flinkt.registry.GeneratedTypeModule`, where [TypeRegistry.load] finds it with
 * `java.util.ServiceLoader`. It needs a public no-argument constructor.
 */
public interface GeneratedTypeModule {
    /** Registers each type this module owns. Called once for each registry built with this module. */
    public fun register(registry: MutableTypeRegistry)
}

/**
 * Where a [GeneratedTypeModule] registers its types. It's only valid while [GeneratedTypeModule.register] runs.
 *
 * A type registered twice, by the same module or by two, fails the registry with an error naming both modules.
 */
public interface MutableTypeRegistry {
    /**
     * Registers a generated declaration: its class, the schema that describes it, and the factory of its codec.
     *
     * The class is the declaration's identity. [schema]'s `typeName` must be its JVM name.
     */
    public fun <T : Any> register(type: KClass<T>, schema: DeclarationSchema, codecFactory: CodecFactory<T>)

    /**
     * Registers the codec of a built-in type, one of [BuiltInKind]'s, named by its Kotlin class, such as `Int::class`.
     *
     * The class and the factory have the same type argument, so a factory for another type doesn't compile, and
     * `codec<Int>()` can't return a codec of something else. A class that isn't a built-in type fails the registry.
     *
     * Flinkt's core doesn't hard-code a built-in's codec, because how each scalar is written is a format decision (#5)
     * that the generated codecs have to agree with.
     */
    public fun <T : Any> registerBuiltIn(type: KClass<T>, codecFactory: CodecFactory<T>)
}
