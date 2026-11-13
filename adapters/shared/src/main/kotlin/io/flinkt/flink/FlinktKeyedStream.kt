package io.flinkt.flink

import io.flinkt.codegen.FlinkView
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

/**
 * A Kotlin view of one Flink [KeyedStream]. It holds that object and nothing else, and [asFlink] returns it.
 *
 * Any keyed stream can enter, wherever it was built, such as by `DataStreamUtils.reinterpretAsKeyedStream`. Its key
 * selector, its key type and, on Flink 2.x, its asynchronous state stay on the Flink object, and [process] calls
 * Flink on that object, so Flink builds the keyed operator it would build for the direct call.
 *
 * It offers the operators of [FlinktDataStream] too, which call Flink on the same object.
 */
@FlinkView(KeyedStream::class, forward = [])
public class FlinktKeyedStream<T, K> internal constructor(
    private val keyed: KeyedStream<T, K>,
) : FlinktDataStream<T>(keyed), FlinktKeyedStreamForwarders<T, K> {
    /** The Flink keyed stream this view holds: the instance that entered with `.flinkt()`, or that `keyBy` returned. */
    override fun asFlink(): KeyedStream<T, K> = keyed

    /**
     * Flink's keyed `process`, with the result type [R] taken from the static type of [keyedProcessFunction], as in
     * `SessionFunction : KeyedProcessFunction<Long, User, Session>`. It calls `process(keyedProcessFunction,
     * typeInfo<R>())`, which is the overload below.
     *
     * A function written in Java carries no nullability, so its result type is non-null. When it can return null,
     * state the type, as in `process<Session?>(javaFunction)`.
     *
     * @throws io.flinkt.types.UnsupportedTypeException if Flinkt has no `TypeInformation` for [R]. Flink isn't called.
     */
    public inline fun <reified R> process(
        keyedProcessFunction: KeyedProcessFunction<K, T, R>,
    ): FlinktSingleOutputStreamOperator<R> = process(keyedProcessFunction, typeInfo<R>())

    /**
     * Flink's keyed `process` with an explicit result type, for code where [R] isn't known at the call site.
     *
     * Flink's `process(keyedProcessFunction, outputType)` is `@Internal` on every target line. So this calls Flink's
     * public `process(keyedProcessFunction)` on the keyed stream this view holds, and then `returns(outputType)` on the
     * operator that call returned, before anything reads its type. Flink's one-argument `process` extracts a result
     * type with missing types allowed, and `returns` replaces it, so Flink builds the operator it builds for the
     * two-argument call (docs/architecture.md, "Views", rule 4).
     */
    public fun <R> process(
        keyedProcessFunction: KeyedProcessFunction<K, T, R>,
        outputType: TypeInformation<R>,
    ): FlinktSingleOutputStreamOperator<R> =
        FlinktSingleOutputStreamOperator(keyed.process(keyedProcessFunction).returns(outputType))
}
