// SPIKE: the first-slice view API with the exact names and signatures the documents use.
package flinkt.docs

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import spike.types.typeInfo

open class FlinktDataStream<T> @PublishedApi internal constructor(
    @PublishedApi internal open val flink: DataStream<T>,
) {
    open fun asFlink(): DataStream<T> = flink

    inline fun <reified R> map(fn: MapFunction<T, R>): FlinktSingleOutputStreamOperator<R> = map(fn, typeInfo<R>())

    fun <R> map(fn: MapFunction<T, R>, outputType: TypeInformation<R>): FlinktSingleOutputStreamOperator<R> =
        FlinktSingleOutputStreamOperator(flink.map(fn, outputType))

    fun filter(fn: FilterFunction<T>): FlinktSingleOutputStreamOperator<T> = FlinktSingleOutputStreamOperator(flink.filter(fn))

    inline fun <reified K> keyBy(fn: KeySelector<T, K>): FlinktKeyedStream<T, K> = keyBy(fn, typeInfo<K>())

    fun <K> keyBy(fn: KeySelector<T, K>, keyType: TypeInformation<K>): FlinktKeyedStream<T, K> =
        FlinktKeyedStream(flink.keyBy(fn, keyType))
}

class FlinktSingleOutputStreamOperator<T> @PublishedApi internal constructor(
    @PublishedApi override val flink: SingleOutputStreamOperator<T>,
) : FlinktDataStream<T>(flink) {
    override fun asFlink(): SingleOutputStreamOperator<T> = flink
    fun name(name: String) = FlinktSingleOutputStreamOperator(flink.name(name))
    fun uid(uid: String) = FlinktSingleOutputStreamOperator(flink.uid(uid))
    fun setParallelism(parallelism: Int) = FlinktSingleOutputStreamOperator(flink.setParallelism(parallelism))
}

class FlinktKeyedStream<T, K> @PublishedApi internal constructor(
    @PublishedApi override val flink: KeyedStream<T, K>,
) : FlinktDataStream<T>(flink) {
    override fun asFlink(): KeyedStream<T, K> = flink

    inline fun <reified R> process(fn: KeyedProcessFunction<K, T, R>): FlinktSingleOutputStreamOperator<R> = process(fn, typeInfo<R>())

    // Flink's process(fn, TypeInformation) is @Internal on every target line.
    fun <R> process(fn: KeyedProcessFunction<K, T, R>, outputType: TypeInformation<R>): FlinktSingleOutputStreamOperator<R> =
        FlinktSingleOutputStreamOperator(flink.process(fn).returns(outputType))
}

fun <T> DataStream<T>.flinkt(): FlinktDataStream<T> = FlinktDataStream(this)
fun <T> SingleOutputStreamOperator<T>.flinkt(): FlinktSingleOutputStreamOperator<T> = FlinktSingleOutputStreamOperator(this)
fun <T, K> KeyedStream<T, K>.flinkt(): FlinktKeyedStream<T, K> = FlinktKeyedStream(this)
