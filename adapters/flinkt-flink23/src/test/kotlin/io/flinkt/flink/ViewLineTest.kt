package io.flinkt.flink

import io.flinkt.flink.StreamGraphs.structure
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.runtime.asyncprocessing.operators.AsyncKeyedProcessOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** The views on what only Flink 2.x has. The shared view tests cover the rest. */
class ViewLineTest {
    private val byParity = KeySelector<Long, Long> { it % 2 }
    private val count = Count()

    @Test
    fun `a keyed stream with async state enabled before entering gets an asynchronous keyed operator, as in Flink`() {
        val direct = StreamExecutionEnvironment.createLocalEnvironment(2).apply {
            fromSequence(1, 100).keyBy(byParity, Types.LONG).enableAsyncState().process(count, Types.INT)
        }
        val view = StreamExecutionEnvironment.createLocalEnvironment(2).apply {
            fromSequence(1, 100).keyBy(byParity, Types.LONG).enableAsyncState().flinkt().process(count)
        }

        val graph = structure(view)
        assertEquals(structure(direct), graph)
        assertEquals(AsyncKeyedProcessOperator::class.java, graph.last().operator)
    }

    class Count : KeyedProcessFunction<Long, Long, Int>() {
        override fun processElement(value: Long, ctx: Context, out: Collector<Int>) = out.collect(1)
    }
}
