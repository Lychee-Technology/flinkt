// SPIKE: hand-written views. Only constructors, asFlink() and type-introducing operators are written here;
// everything that merely forwards is generated into <View>Forwarders from the Flink class on the classpath.
package flinkt.views

import flinkt.codegen.FlinkView
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

@FlinkView(
    DataStream::class,
    forward = [
        "filter", "union", "rebalance", "rescale", "shuffle", "forward", "global", "partitionCustom",
        "assignTimestampsAndWatermarks", "sinkTo", "print", "printToErr", "executeAndCollect",
    ],
)
public open class FlinktDataStream<T> @PublishedApi internal constructor(
    @PublishedApi internal open val flink: DataStream<T>,
) : FlinktDataStreamForwarders<T> {
    override fun asFlink(): DataStream<T> = flink

    public inline fun <reified R> map(fn: MapFunction<T, R>): FlinktSingleOutputStreamOperator<R> = map(fn, typeInfo<R>())

    public fun <R> map(fn: MapFunction<T, R>, outputType: TypeInformation<R>): FlinktSingleOutputStreamOperator<R> =
        FlinktSingleOutputStreamOperator(flink.map(fn, outputType))

    public inline fun <reified K> keyBy(fn: KeySelector<T, K>): FlinktKeyedStream<T, K> = keyBy(fn, typeInfo<K>())

    public fun <K> keyBy(fn: KeySelector<T, K>, keyType: TypeInformation<K>): FlinktKeyedStream<T, K> =
        FlinktKeyedStream(flink.keyBy(fn, keyType))
}

@FlinkView(
    SingleOutputStreamOperator::class,
    forward = [
        "name", "uid", "setUidHash", "setDescription", "setParallelism", "setMaxParallelism", "forceNonParallel",
        "setBufferTimeout", "slotSharingGroup", "startNewChain", "disableChaining",
    ],
)
public class FlinktSingleOutputStreamOperator<T> @PublishedApi internal constructor(
    @PublishedApi override val flink: SingleOutputStreamOperator<T>,
) : FlinktDataStream<T>(flink), FlinktSingleOutputStreamOperatorForwarders<T> {
    override fun asFlink(): SingleOutputStreamOperator<T> = flink
}

@FlinkView(KeyedStream::class, forward = ["reduce"])
public class FlinktKeyedStream<T, K> @PublishedApi internal constructor(
    @PublishedApi override val flink: KeyedStream<T, K>,
) : FlinktDataStream<T>(flink), FlinktKeyedStreamForwarders<T, K> {
    override fun asFlink(): KeyedStream<T, K> = flink

    public inline fun <reified R> process(fn: KeyedProcessFunction<K, T, R>): FlinktSingleOutputStreamOperator<R> =
        process(fn, typeInfo<R>())

    // Flink's process(fn, TypeInformation) is @Internal on every target line.
    public fun <R> process(fn: KeyedProcessFunction<K, T, R>, outputType: TypeInformation<R>): FlinktSingleOutputStreamOperator<R> =
        FlinktSingleOutputStreamOperator(flink.process(fn).returns(outputType))
}

public fun <T> DataStream<T>.flinkt(): FlinktDataStream<T> = FlinktDataStream(this)
public fun <T> SingleOutputStreamOperator<T>.flinkt(): FlinktSingleOutputStreamOperator<T> = FlinktSingleOutputStreamOperator(this)
public fun <T, K> KeyedStream<T, K>.flinkt(): FlinktKeyedStream<T, K> = FlinktKeyedStream(this)
