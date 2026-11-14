package io.flinkt.flink

import io.flinkt.flink.StreamGraphs.structure
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.runtime.asyncprocessing.operators.AsyncKeyedProcessOperator
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.StreamGroupedReduceAsyncStateOperator
import org.apache.flink.streaming.api.operators.StreamGroupedReduceOperator
import org.apache.flink.streaming.api.transformations.ReduceTransformation
import org.apache.flink.util.Collector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/** The views on what only Flink 2.x has. The shared view tests cover the rest. */
class ViewLineTest {
    private val byParity = KeySelector<Long, Long> { it % 2 }
    private val count = Count()
    private val sum = ReduceFunction<Long> { a, b -> a + b }

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

    @Test
    fun `an operator with async state enabled before entering still has it after a fluent call through the view`() {
        // SingleOutputStreamOperator.enableAsyncState() sets the flag on the operator's transformation, which a keyed
        // reduce has, and Flink then builds the asynchronous reduce operator.
        lateinit var left: SingleOutputStreamOperator<Long>
        val view = StreamExecutionEnvironment.createLocalEnvironment(2).apply {
            val reduced = fromSequence(1, 100).keyBy(byParity, Types.LONG).reduce(sum).enableAsyncState()
            left = reduced.flinkt().name("sum").uid("sum-v1").asFlink()
            assertSame(reduced, left)
        }
        val direct = StreamExecutionEnvironment.createLocalEnvironment(2).apply {
            fromSequence(1, 100).keyBy(byParity, Types.LONG).reduce(sum).enableAsyncState().name("sum").uid("sum-v1")
        }
        val synchronous = StreamExecutionEnvironment.createLocalEnvironment(2).apply {
            fromSequence(1, 100).keyBy(byParity, Types.LONG).reduce(sum).flinkt().name("sum").uid("sum-v1")
        }

        // getTransformation() is @Internal, so only the test reads it.
        assertTrue((left.transformation as ReduceTransformation<*, *>).isEnableAsyncState)
        val graph = structure(view)
        assertEquals(structure(direct), graph)
        assertEquals(StreamGroupedReduceAsyncStateOperator::class.java, graph.last().operator)
        // The control: without the flag, Flink builds the synchronous reduce operator.
        assertEquals(StreamGroupedReduceOperator::class.java, structure(synchronous).last().operator)
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `enableAsyncState isn't offered by any view, and is reached through asFlink()`(compiler: KotlinCompiler) {
        // Flink 2.x's async state (API v2) is out of scope for #31, and a forwarder would make it Flinkt's API.
        for (view in listOf("FlinktDataStream<String>", "FlinktSingleOutputStreamOperator<String>", "FlinktKeyedStream<String, Int>")) {
            KotlinCompilation(compiler)
                .compile("${IMPORTS}fun asyncState(view: $view) = view.enableAsyncState()")
                .assertFailure("UNRESOLVED_REFERENCE", "Unresolved reference 'enableAsyncState'")
        }
        KotlinCompilation(compiler)
            .compile(
                IMPORTS +
                    """
                    fun asyncState(operator: FlinktSingleOutputStreamOperator<String>, keyed: FlinktKeyedStream<String, Int>) {
                        operator.asFlink().enableAsyncState()
                        keyed.asFlink().enableAsyncState()
                    }
                    """.trimIndent(),
            )
            .assertSuccess()
    }

    class Count : KeyedProcessFunction<Long, Long, Int>() {
        override fun processElement(value: Long, ctx: Context, out: Collector<Int>) = out.collect(1)
    }

    private companion object {
        const val IMPORTS = "import io.flinkt.flink.FlinktDataStream\n" +
            "import io.flinkt.flink.FlinktKeyedStream\n" +
            "import io.flinkt.flink.FlinktSingleOutputStreamOperator\n\n"
    }
}
