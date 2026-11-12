package io.flinkt.codec

import io.flinkt.types.FlinktType
import java.io.DataInput
import java.io.DataOutput
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Reads, writes and copies values of one type, for Flinkt's adapters to call.
 *
 * KSP generates one for each `@FlinkType` declaration. It reads fields and calls the constructor directly, without
 * reflection, and mentions no Flink type, so it doesn't change with the Flink line. Each adapter wraps it in the
 * `TypeSerializer` it shares between all generated types; Flink's `DataInputView` and `DataOutputView` are a
 * [DataInput] and a [DataOutput], so the adapter passes them through.
 *
 * Which bytes a codec writes is the format's decision (#5), not this interface's. A codec's bytes must not depend on
 * the output it writes to, and Flink's output views don't all write the same bytes for the same call, so a codec
 * converts floating-point values itself rather than calling `writeFloat` or `writeDouble`
 * (docs/architecture.md, "Generated code and the adapter").
 *
 * A codec holds no state that changes between calls: one instance can serve concurrent callers. The codecs it needs
 * for nested types are fixed when it's created ([CodecFactory]), never looked up per value.
 */
public interface GeneratedCodec<T> {
    /** Writes [value] to [output]. */
    public fun serialize(value: T, output: DataOutput)

    /** Reads one value from [input], as [serialize] wrote it. */
    public fun deserialize(input: DataInput): T

    /**
     * Returns a value equal to [value] that shares no mutable object with it, so that neither changes when the other
     * is mutated. A deeply immutable value may be returned as it is.
     */
    public fun copy(value: T): T
}

/**
 * Creates the codec of one type. A `GeneratedTypeModule` registers one for each declaration it owns.
 *
 * The factory runs when the type is resolved, at job-graph construction or serializer creation, never per value.
 * Everything the codec needs from other types, it takes from the [CodecContext] then and keeps.
 */
public fun interface CodecFactory<T> {
    public fun create(context: CodecContext): GeneratedCodec<T>
}

/**
 * What a [CodecFactory] gets when it creates a codec: the type it creates it for, the codecs of that type's
 * arguments, and the codecs of the types its values contain.
 *
 * A context is valid only while [CodecFactory.create] runs. A codec that kept it to look codecs up per value would
 * fail, so nested codecs are always fixed at creation.
 */
public interface CodecContext {
    /** The type the codec is created for, with its type arguments. */
    public val type: FlinktType

    /**
     * The codecs of [type]'s arguments, one per type parameter of the declaration and in the same order, created
     * before the factory runs. A codec for `Envelope<T>` gets the codec of its `T` here. Empty for a non-generic type.
     */
    public val argumentCodecs: List<GeneratedCodec<*>>

    /**
     * Resolves the codec of a type that values of [type] contain, such as a field's type. The codec of a record
     * calls it once per field type when it's created and keeps the result.
     *
     * Fails with `UnsupportedTypeException` when the type can't be resolved, and so does the creation of this codec.
     */
    public fun codec(type: KType): GeneratedCodec<*>
}

/** Resolves the codec of [T], a type that values of the context's type contain. See [CodecContext.codec]. */
@Suppress("UNCHECKED_CAST")
public inline fun <reified T> CodecContext.codec(): GeneratedCodec<T> = codec(typeOf<T>()) as GeneratedCodec<T>
