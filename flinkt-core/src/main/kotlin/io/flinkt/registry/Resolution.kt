package io.flinkt.registry

import io.flinkt.codec.GeneratedCodec
import io.flinkt.types.BuiltInKind
import io.flinkt.types.FlinktType

/**
 * What supported resolution ([TypeRegistry.resolve]) gives for a type Flinkt supports: a built-in's kind, or the
 * codec of a type whose bytes Flinkt's generated code writes.
 *
 * A type Flinkt doesn't support has no resolution: resolving it throws `UnsupportedTypeException` instead.
 */
public sealed class Resolution {
    /** The type that was resolved, as the resolving registry models it. */
    public abstract val type: FlinktType

    /**
     * A built-in type, which resolves to its [kind] and to no Flinkt codec. The adapter maps the kind to Flink's own
     * `TypeInformation`, so a built-in's bytes are Flink's, and no module, Flinkt's core included, defines them.
     */
    public class BuiltIn internal constructor(
        override val type: FlinktType,
        public val kind: BuiltInKind,
    ) : Resolution() {
        override fun equals(other: Any?): Boolean =
            this === other || other is BuiltIn && kind == other.kind && type == other.type

        override fun hashCode(): Int = kind.hashCode() * 31 + type.hashCode()

        override fun toString(): String = "BuiltIn($type, $kind)"
    }

    /**
     * A type that Flinkt writes with [codec], such as a non-generic generated record, whose codec its module's
     * factory created with every nested codec it needs.
     */
    public class Codec internal constructor(
        override val type: FlinktType,
        public val codec: GeneratedCodec<*>,
    ) : Resolution() {
        override fun toString(): String = "Codec($type, $codec)"
    }
}
