package io.flinkt.registry

import io.flinkt.codec.CodecFactory
import io.flinkt.schema.DeclarationSchema
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
 * A module registers generated declarations only. A built-in type resolves to its built-in kind, whose bytes are
 * Flink's, so a module can't register a codec for one: a jar on the classpath must not decide how an `Int` or a
 * `String` is written.
 *
 * A type registered twice, by the same module or by two, fails the registry with an error naming both modules.
 */
public interface MutableTypeRegistry {
    /**
     * Registers a generated declaration: its class, the schema that describes it, and the factory of its codec.
     *
     * The class is the declaration's identity. [schema]'s `typeName` must be its JVM name. A built-in type fails the
     * registry.
     */
    public fun <T : Any> register(type: KClass<T>, schema: DeclarationSchema, codecFactory: CodecFactory<T>)
}
