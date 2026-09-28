// SPIKE (disposable): composition view. Each view holds exactly one Flink object and never builds one.
// Every method makes one call on that object and wraps exactly the object Flink returns.
package spike.compose

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.OutputTag
import spike.types.typeInfo

open class FlinktStream<T> @PublishedApi internal constructor(
    @PublishedApi internal open val flink: DataStream<T>,
) {
    open fun asFlink(): DataStream<T> = flink

    inline fun <reified R> map(fn: MapFunction<T, R>): FlinktOperator<R> =
        FlinktOperator(flink.map(fn, typeInfo<R>()))

    inline fun <reified R> flatMap(fn: FlatMapFunction<T, R>): FlinktOperator<R> =
        FlinktOperator(flink.flatMap(fn, typeInfo<R>()))

    // Flink's process(fn, TypeInformation) is @Internal in 1.20, 2.2 and 2.3, so use the public returns().
    inline fun <reified R> process(fn: ProcessFunction<T, R>): FlinktOperator<R> =
        FlinktOperator(flink.process(fn).returns(typeInfo<R>()))

    fun <R> process(fn: ProcessFunction<T, R>, outputType: TypeInformation<R>): FlinktOperator<R> =
        FlinktOperator(flink.process(fn).returns(outputType))

    fun filter(fn: FilterFunction<T>): FlinktOperator<T> = FlinktOperator(flink.filter(fn))

    inline fun <reified K> keyBy(fn: KeySelector<T, K>): FlinktKeyedStream<T, K> =
        FlinktKeyedStream(flink.keyBy(fn, typeInfo<K>()))

    fun union(vararg others: FlinktStream<T>): FlinktStream<T> {
        @Suppress("UNCHECKED_CAST")
        val inputs = others.map { it.flink }.toTypedArray<DataStream<T>>()
        return FlinktStream(flink.union(*inputs))
    }

    fun rebalance(): FlinktStream<T> = FlinktStream(flink.rebalance())
}

class FlinktOperator<T> @PublishedApi internal constructor(
    @PublishedApi override val flink: SingleOutputStreamOperator<T>,
) : FlinktStream<T>(flink) {
    override fun asFlink(): SingleOutputStreamOperator<T> = flink

    fun name(name: String): FlinktOperator<T> = FlinktOperator(flink.name(name))
    fun uid(uid: String): FlinktOperator<T> = FlinktOperator(flink.uid(uid))
    fun setParallelism(parallelism: Int): FlinktOperator<T> = FlinktOperator(flink.setParallelism(parallelism))
    fun forceNonParallel(): FlinktOperator<T> = FlinktOperator(flink.forceNonParallel())

    inline fun <reified X> getSideOutput(tag: OutputTag<X>): FlinktStream<X> = FlinktStream(flink.getSideOutput(tag))
}

class FlinktKeyedStream<T, K> @PublishedApi internal constructor(
    @PublishedApi override val flink: KeyedStream<T, K>,
) : FlinktStream<T>(flink) {
    override fun asFlink(): KeyedStream<T, K> = flink

    inline fun <reified R> process(fn: KeyedProcessFunction<K, T, R>): FlinktOperator<R> =
        FlinktOperator(flink.process(fn).returns(typeInfo<R>()))
}

fun <T> DataStream<T>.flinkt(): FlinktStream<T> = FlinktStream(this)
fun <T> SingleOutputStreamOperator<T>.flinkt(): FlinktOperator<T> = FlinktOperator(this)
fun <T, K> KeyedStream<T, K>.flinkt(): FlinktKeyedStream<T, K> = FlinktKeyedStream(this)
