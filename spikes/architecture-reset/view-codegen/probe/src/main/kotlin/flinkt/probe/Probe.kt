// SPIKE: exercises the generated forwarders on a real Flink line.
package flinkt.probe

import flinkt.views.FlinktDataStream
import flinkt.views.FlinktKeyedStream
import flinkt.views.FlinktSingleOutputStreamOperator
import flinkt.views.flinkt
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

internal class Exactly<T>
internal fun <T> exactTypeOf(@Suppress("UNUSED_PARAMETER") value: T): Exactly<T> = Exactly()
internal fun <U> Exactly<U>.shouldBe() = Unit

internal fun outcome(block: () -> Any?) = runCatching { block(); "accepted" }.getOrElse { "rejected (${it.javaClass.simpleName})" }

public fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 2
    val words = env.fromData(listOf("a", "bb", "cc", "ddd"), Types.STRING).flinkt()
    val more = env.fromData(listOf("eeee"), Types.STRING).flinkt()

    // Generated members keep the chain in the view; hand-written ones introduce types.
    val lengths =
        words
            .filter { it.isNotEmpty() }            // generated: FilterFunction<T> -> view
            .union(more)                           // generated: Flink's union is final; forwarding it is fine
            .rebalance()                           // generated
            .map { it.length.toLong() }            // hand-written, reified
            .name("lengths")                       // generated fluent
            .uid("lengths-v1")
            .setParallelism(2)
            .keyBy { it }                          // hand-written, reified
            .reduce { a, b -> a + b }              // generated: ReduceFunction<T> -> view
    exactTypeOf(words.filter { true }).shouldBe<FlinktSingleOutputStreamOperator<String>>()
    exactTypeOf(words.union(more)).shouldBe<FlinktDataStream<String>>()
    exactTypeOf(lengths).shouldBe<FlinktSingleOutputStreamOperator<Long>>()
    exactTypeOf(lengths.keyBy { it }).shouldBe<FlinktKeyedStream<Long, Long>>()
    exactTypeOf(lengths.print()).shouldBe<DataStreamSink<Long>>()

    println("result: ${lengths.executeAndCollect(10).sorted()}")

    // A forwarded call reaches the object the view holds, so Flink's own checks apply.
    val op = env.fromData(listOf(1L), Types.LONG).map(MapFunction<Long, Long> { it }, Types.LONG)
    op.forceNonParallel()
    println("original setMaxParallelism(2): ${outcome { op.setMaxParallelism(2) }}")
    println("view (generated) setMaxParallelism(2): ${outcome { op.flinkt().setMaxParallelism(2) }}")
    println("view is the same object: ${op.flinkt().asFlink() === op}")
}
