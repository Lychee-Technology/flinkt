package io.flinkt.flink

import io.flinkt.flink.StreamGraphs.structure
import io.flinkt.flink.StreamGraphs.transformations
import io.flinkt.types.UnsupportedTypeException
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.operators.StreamMap
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * The views' adapter contract (docs/testing.md, "Flink adapter contract"), on the adapter's own Flink line: what the
 * views call on Flink, shown by the stream graphs they build, without running them. Until generated types have a
 * `TypeInformation` (#12), the pipelines use built-in element types.
 */
class ViewContractTest {
    private val toText = ToText()
    private val byLength = KeySelector<String, Int> { it.length }
    private val count = CountByKey()

    @Test
    fun `map, keyBy and keyed process build the graph that direct Flink calls with the expected TypeInformation build`() {
        val direct = environment().apply {
            fromSequence(1, 100).map(toText, Types.STRING).keyBy(byLength, Types.INT).process(count, Types.LONG)
        }
        val reified = environment().apply { fromSequence(1, 100).flinkt().map(toText).keyBy(byLength).process(count) }
        val explicit = environment().apply {
            fromSequence(1, 100).flinkt().map(toText, Types.STRING).keyBy(byLength, Types.INT).process(count, Types.LONG)
        }

        val graph = structure(direct)
        assertEquals(graph, structure(reified))
        assertEquals(graph, structure(explicit))
        // What the graph is, so that the comparison can't pass on two empty or untyped graphs.
        assertEquals(listOf("Source: Sequence Source", "Map", "KeyedProcess"), graph.map { it.name })
        val (source, map, process) = graph
        assertEquals(listOf(StreamGraphs.Edge(1, ForwardPartitioner::class.java, 0)), source.edges)
        assertEquals(listOf(StreamGraphs.Edge(2, KeyGroupStreamPartitioner::class.java, 0)), map.edges)
        assertEquals(StreamMap::class.java, map.operator)
        assertSame(toText, map.function)
        assertEquals(StringSerializer.INSTANCE, map.outputSerializer)
        assertEquals(KeyedProcessOperator::class.java, process.operator)
        assertSame(count, process.function)
        assertEquals(listOf(byLength), process.keySelectors)
        assertEquals(IntSerializer.INSTANCE, process.keySerializer)
        assertEquals(LongSerializer.INSTANCE, process.outputSerializer)
    }

    @Test
    fun `a generic function class gets its result type from the call site, which Flink alone can't infer`() {
        val env = environment()
        val keyed = env.fromSequence(1, 100).map(toText, Types.STRING).keyBy(byLength, Types.INT)

        val passed = keyed.flinkt().map(Identity()).keyBy(byLength).process(Passthrough())
        assertEquals(BasicTypeInfo.STRING_TYPE_INFO, passed.asFlink().type)
        // Flink's own process(fn) can't infer it: the result type is the class's type parameter.
        assertThrows<InvalidTypesException> { keyed.process(Passthrough<Int, String>()).type }
    }

    @Test
    fun `the operators accept Flink's rich function variants`() {
        val rich = object : RichMapFunction<Long, String>() {
            override fun map(value: Long): String = value.toString()
        }
        // KeyedProcessFunction is a rich function itself.
        assertTrue(RichFunction::class.java.isAssignableFrom(KeyedProcessFunction::class.java))
        val direct = environment().apply {
            fromSequence(1, 100).map(rich, Types.STRING).keyBy(byLength, Types.INT).process(count, Types.LONG)
        }
        val view = environment().apply { fromSequence(1, 100).flinkt().map(rich).keyBy(byLength).process(count) }

        val graph = structure(view)
        assertEquals(structure(direct), graph)
        assertSame(rich, graph[1].function)
    }

    @Test
    fun `a keyed stream from reinterpretAsKeyedStream keeps its key selector and key type, with no extra partition step`() {
        val bySelf = KeySelector<Long, Long> { it }
        val passthrough = Passthrough<Long, Long>()
        val direct = environment().apply {
            DataStreamUtils.reinterpretAsKeyedStream(fromSequence(1, 100), bySelf, Types.LONG).process(passthrough, Types.LONG)
        }
        val view = environment().apply {
            DataStreamUtils.reinterpretAsKeyedStream(fromSequence(1, 100), bySelf, Types.LONG).flinkt().process(passthrough)
        }

        val graph = structure(view)
        assertEquals(structure(direct), graph)
        val (source, process) = graph
        // Forward, as reinterpretAsKeyedStream partitions it. A keyed stream rebuilt by Flinkt would add a hash shuffle.
        assertEquals(listOf(StreamGraphs.Edge(1, ForwardPartitioner::class.java, 0)), source.edges)
        assertEquals(KeyedProcessOperator::class.java, process.operator)
        assertEquals(listOf(bySelf), process.keySelectors)
        assertEquals(LongSerializer.INSTANCE, process.keySerializer)
    }

    @Test
    fun `entering returns the instance that entered, and changes nothing in the environment`() {
        val env = environment()
        val source = env.fromSequence(1, 100)
        val operator = source.process(Split(), Types.LONG)
        val sideOutput = operator.getSideOutput(Split.ODD)
        val union: DataStream<Long> = source.union(operator)
        val keyed = operator.keyBy(KeySelector<Long, Long> { it % 10 }, Types.LONG)

        val entered: List<Pair<Any, () -> Any>> = listOf(
            source to { source.flinkt().asFlink() },
            operator to { operator.flinkt().asFlink() },
            sideOutput to { sideOutput.flinkt().asFlink() },
            union to { union.flinkt().asFlink() },
            keyed to { keyed.flinkt().asFlink() },
        )
        for ((stream, enterAndLeave) in entered) {
            val before = transformations(env)
            assertSame(stream, enterAndLeave(), stream.javaClass.name)
            assertSameElements(before, transformations(env))
        }
    }

    @Test
    fun `entering, and wrapping what Flink returned, read nothing, so Flink still accepts returns() on the stream`() {
        val env = environment()
        val longs = GenericTypeInfo(Long::class.javaObjectType)
        val texts = GenericTypeInfo(String::class.java)
        // A source and an operator, each with an output type Flink inferred. Each has a source of its own, because
        // building an operator reads the type of its input.
        val source = env.fromSequence(1, 100)
        val operator = env.fromSequence(1, 100).map(toText)
        source.flinkt().asFlink().returns(longs)
        operator.flinkt().asFlink().returns(texts)
        assertSame(longs, source.type)
        assertSame(texts, operator.type)

        // A view operator wraps what Flink returned, and reads nothing from it either.
        val mapped = env.fromSequence(1, 100).flinkt().map(toText).asFlink()
        val processed = env.fromSequence(1, 100).flinkt().map(toText).keyBy(byLength).process(count).asFlink()
        mapped.returns(texts)
        processed.returns(longs)
        assertSame(texts, mapped.type)
        assertSame(longs, processed.type)

        // The control: once its type has been read, Flink refuses returns().
        val read = env.fromSequence(1, 100).map(toText)
        read.type
        assertThrows<IllegalStateException> { read.returns(texts) }
    }

    @Test
    fun `a result type typeInfo can't resolve fails before Flink is called`() {
        val env = environment()
        val view = env.fromSequence(1, 100).flinkt()
        val before = transformations(env)

        val e = assertThrows<UnsupportedTypeException> { view.map { it.toString().takeIf { text -> text.isNotEmpty() } } }
        assertEquals("kotlin.String?", e.canonicalType)
        assertSameElements(before, transformations(env))
    }

    private fun environment(): StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(2)

    private fun assertSameElements(expected: List<*>, actual: List<*>) {
        assertEquals(expected.size, actual.size, "$actual")
        expected.zip(actual).forEach { (e, a) -> assertSame(e, a) }
    }

    /** A Kotlin function class, whose result type Flink's `TypeExtractor` infers. */
    class ToText : MapFunction<Long, String> {
        override fun map(value: Long): String = "event-$value"
    }

    class Identity<X> : MapFunction<X, X> {
        override fun map(value: X): X = value
    }

    class CountByKey : KeyedProcessFunction<Int, String, Long>() {
        override fun processElement(value: String, ctx: Context, out: Collector<Long>) = out.collect(1)
    }

    class Passthrough<K, X> : KeyedProcessFunction<K, X, X>() {
        override fun processElement(value: X, ctx: Context, out: Collector<X>) = out.collect(value)
    }

    /** Emits even values, and odd ones to the side output [ODD]. */
    class Split : ProcessFunction<Long, Long>() {
        override fun processElement(value: Long, ctx: Context, out: Collector<Long>) {
            if (value % 2 == 0L) out.collect(value) else ctx.output(ODD, value.toString())
        }

        companion object {
            val ODD = OutputTag("odd", Types.STRING)
        }
    }
}
