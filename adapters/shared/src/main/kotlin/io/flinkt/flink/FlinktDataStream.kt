package io.flinkt.flink

import io.flinkt.codegen.FlinkView
import kotlin.reflect.typeOf
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream

/**
 * A Kotlin view of one Flink [DataStream]. It holds that object and nothing else, and [asFlink] returns it.
 *
 * `.flinkt()` gives the view of a stream. Entering reads nothing from the stream, not even its type, and adds nothing
 * to the job. A view isn't a `DataStream`: a Flink method the view doesn't offer is a compile error on it, never a
 * silent switch to Flink's own method, and is called on [asFlink] instead.
 *
 * Each view method calls the Flink method of the same name once, on the object the view holds, and wraps the object
 * that call returned. So Flink's validation, partitioning and operator selection apply as they would to the direct
 * call. [map] and [keyBy] introduce an element type. Each takes Flink's own function type, which a Kotlin lambda
 * converts to and a function object passes as it is, so the compiler knows the result type at the call site, and the
 * view gives Flink `typeInfo()` for it. Each has an overload that takes the `TypeInformation` explicitly, for generic
 * code where the type isn't known at the call site. See docs/architecture.md, "Views".
 */
@FlinkView(DataStream::class, forward = [])
public open class FlinktDataStream<T> internal constructor(
    private val stream: DataStream<T>,
) : FlinktDataStreamForwarders<T> {
    /** The Flink stream this view holds: the instance that entered with `.flinkt()`, or that a Flink call returned. */
    override fun asFlink(): DataStream<T> = stream

    /**
     * Flink's `map`, with the result type [R] taken from the call site: from the lambda's result, or from the static
     * type of [mapper]. It calls `map(mapper, typeInfo<R>())` on the stream this view holds.
     *
     * A function written in Java carries no nullability, so its result type is non-null. When it can return null,
     * state the type, as in `map<User?>(javaFunction)`.
     *
     * @throws io.flinkt.types.UnsupportedTypeException if Flinkt has no `TypeInformation` for [R]. Flink isn't called.
     */
    public inline fun <reified R> map(mapper: MapFunction<T, R>): FlinktSingleOutputStreamOperator<R> =
        map(mapper, typeInfo<R>())

    /** Flink's `map(mapper, outputType)` on the stream this view holds, for code where [R] isn't known at the call site. */
    public fun <R> map(mapper: MapFunction<T, R>, outputType: TypeInformation<R>): FlinktSingleOutputStreamOperator<R> =
        FlinktSingleOutputStreamOperator(stream.map(mapper, outputType))

    /**
     * Flink's `keyBy`, with the key type [K] taken from the call site: from the lambda's result, or from the static
     * type of [key]. It calls `keyBy(key, typeInfo<K>())` on the stream this view holds.
     *
     * Until Flinkt's key rules are implemented (docs/architecture.md, "Keys"), it accepts only a non-null `Boolean`,
     * `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Char` or `String` key. It checks [K] before it looks up a
     * `TypeInformation` for it, and refuses every other key type, whether or not Flinkt has a `TypeInformation` for
     * it. Flink's own rules for other key types are reached through `asFlink().keyBy(…)`.
     *
     * @throws IllegalArgumentException if [K] isn't one of those types. Flink isn't called.
     */
    public inline fun <reified K> keyBy(key: KeySelector<T, K>): FlinktKeyedStream<T, K> {
        @Suppress("UNCHECKED_CAST")
        return keyBy(key, keyTypeInfo(typeOf<K>()) as TypeInformation<K>)
    }

    /**
     * Flink's `keyBy(key, keyType)` on the stream this view holds, for code where [K] isn't known at the call site.
     *
     * Until Flinkt's key rules are implemented, [keyType] has to be Flink's `BasicTypeInfo` for a non-null `Boolean`,
     * `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Char` or `String`, the one `typeInfo()` gives for it. Flink's
     * own rules for other key types are reached through `asFlink().keyBy(…)`.
     *
     * @throws IllegalArgumentException if [keyType] is any other `TypeInformation`. Flink isn't called.
     */
    public fun <K> keyBy(key: KeySelector<T, K>, keyType: TypeInformation<K>): FlinktKeyedStream<T, K> {
        checkKeyType(keyType)
        return FlinktKeyedStream(stream.keyBy(key, keyType))
    }
}
