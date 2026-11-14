package io.flinkt.flink

import io.flinkt.flink.StreamGraphs.structure
import io.flinkt.flink.StreamGraphs.transformations
import io.flinkt.types.UnsupportedTypeException
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.SlotSharingGroup
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.typeutils.base.StringComparator
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink
import org.apache.flink.streaming.api.operators.ChainingStrategy
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.operators.StreamMap
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
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
    private val nonEmpty = FilterFunction<String> { it.isNotEmpty() }
    private val split = Split()

    @Test
    fun `map, keyBy and keyed process build the graph that direct Flink calls with the expected TypeInformation build`() {
        val direct = environment().apply {
            fromSequence(1, 100).map(toText, Types.STRING).keyBy(byLength, Types.INT).process(count, Types.LONG)
        }
        val reified = environment().apply { fromSequence(1, 100).flinkt().map(toText).keyBy(byLength).process(count) }
        val explicit = environment().apply {
            fromSequence(1, 100).flinkt().map(toText, Types.STRING).keyBy(byLength, Types.INT).process(count, Types.LONG)
        }

        val graphs = listOf("direct" to direct, "reified" to reified, "explicit" to explicit).map { (name, env) ->
            name to structure(env)
        }
        val graph = graphs.first().second
        for ((name, built) in graphs) {
            assertEquals(graph, built, name)
        }
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
        // The TypeInformation each operator got, in each graph: the view's map and keyed process, reified and
        // explicit, give Flink the type the direct calls give it, which is Flink's own constant for a built-in.
        for ((name, built) in graphs) {
            val (_, builtMap, builtProcess) = built
            assertSame(BasicTypeInfo.STRING_TYPE_INFO, builtMap.outputType, name)
            assertSame(BasicTypeInfo.INT_TYPE_INFO, builtProcess.keyType, name)
            assertSame(BasicTypeInfo.LONG_TYPE_INFO, builtProcess.outputType, name)
        }
    }

    @Test
    fun `the graph comparison tells apart two output types whose serializers are equal`() {
        val expected = structure(environment().apply { fromSequence(1, 100).map(toText, Types.STRING) })
        val other = structure(environment().apply { fromSequence(1, 100).map(toText, OtherStringType()) })

        // Only the output type differs, so a comparison of serializers alone would pass.
        assertEquals(expected[1].outputSerializer, other[1].outputSerializer)
        assertEquals(expected, other.map { if (it.outputType is OtherStringType) it.copy(outputType = Types.STRING) else it })
        assertNotEquals(expected, other)
        assertNotEquals(other, expected)
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
    fun `a chain with forwarded name, uid, setParallelism and union builds the graph that direct Flink calls build`() {
        val direct = environment().apply {
            val texts = fromSequence(1, 100).map(toText, Types.STRING).name("texts").uid("texts-v1").setParallelism(3)
            val more = fromSequence(1, 100).map(toText, Types.STRING).name("more").uid("more-v1").setMaxParallelism(64)
            texts.union(more).rebalance().filter(nonEmpty).name("non-empty").uid("non-empty-v1").setParallelism(2)
                .keyBy(byLength, Types.INT).process(count, Types.LONG).name("count").uid("count-v1")
        }
        val view = environment().apply {
            val texts = fromSequence(1, 100).flinkt().map(toText).name("texts").uid("texts-v1").setParallelism(3)
            val more = fromSequence(1, 100).flinkt().map(toText).name("more").uid("more-v1").setMaxParallelism(64)
            texts.union(more).rebalance().filter(nonEmpty).name("non-empty").uid("non-empty-v1").setParallelism(2)
                .keyBy(byLength).process(count).name("count").uid("count-v1")
        }

        val graph = structure(view)
        assertEquals(structure(direct), graph)
        // What the graph is, so that the comparison can't pass on two graphs that both lost the configuration.
        assertEquals(listOf("Source: Sequence Source", "texts", "Source: Sequence Source", "more", "non-empty", "count"), graph.map { it.name })
        assertEquals(listOf(null, "texts-v1", null, "more-v1", "non-empty-v1", "count-v1"), graph.map { it.uid })
        assertEquals(listOf(2, 3, 2, 2, 2, 2), graph.map { it.parallelism })
        assertEquals(64, graph[3].maxParallelism)
        // union adds no node: both inputs reach the filter, through the rebalance that follows it.
        val rebalance = StreamGraphs.Edge(4, RebalancePartitioner::class.java, 0)
        assertEquals(listOf(rebalance), graph[1].edges)
        assertEquals(listOf(rebalance), graph[3].edges)
        assertSame(nonEmpty, graph[4].function)
    }

    @Test
    fun `the operator's forwarded configuration calls set what the direct calls set`() {
        val group = SlotSharingGroup.newBuilder("filters").build()
        val direct = environment().apply {
            fromSequence(1, 100).map(toText, Types.STRING)
                .name("texts").uid("texts-v1").setUidHash(UID_HASH).setDescription("Texts of the sequence")
                .setParallelism(3).setMaxParallelism(64).setBufferTimeout(10).disableChaining().slotSharingGroup("texts")
                .filter(nonEmpty).startNewChain().slotSharingGroup(group)
                .filter(nonEmpty).forceNonParallel()
        }
        val view = environment().apply {
            fromSequence(1, 100).flinkt().map(toText)
                .name("texts").uid("texts-v1").setUidHash(UID_HASH).setDescription("Texts of the sequence")
                .setParallelism(3).setMaxParallelism(64).setBufferTimeout(10).disableChaining().slotSharingGroup("texts")
                .filter(nonEmpty).startNewChain().slotSharingGroup(group)
                .filter(nonEmpty).forceNonParallel()
        }

        val graph = structure(view)
        assertEquals(structure(direct), graph)
        val (_, texts, first, second) = graph
        assertEquals("texts-v1", texts.uid)
        assertEquals(UID_HASH, texts.uidHash)
        assertEquals("Texts of the sequence", texts.description)
        assertEquals(10, texts.bufferTimeout)
        assertEquals(ChainingStrategy.NEVER, texts.chainingStrategy)
        assertEquals("texts", texts.slotSharingGroup)
        assertEquals(ChainingStrategy.HEAD, first.chainingStrategy)
        assertEquals("filters", first.slotSharingGroup)
        assertEquals(1 to 1, second.parallelism to second.maxParallelism)
    }

    @Test
    fun `the graph comparison tells apart each setting of the operator's forwarded configuration calls`() {
        // The control for the tests above: each forwarded configuration call changes what the comparison sees.
        fun configured(configure: SingleOutputStreamOperator<String>.() -> Any) =
            structure(environment().apply { fromSequence(1, 100).map(toText, Types.STRING).configure() })
        val plain = configured {}
        val settings: List<Pair<String, SingleOutputStreamOperator<String>.() -> Any>> = listOf(
            "name" to { name("texts") },
            "uid" to { uid("texts-v1") },
            "setUidHash" to { setUidHash(UID_HASH) },
            "setDescription" to { setDescription("Texts of the sequence") },
            "setParallelism" to { setParallelism(3) },
            "setMaxParallelism" to { setMaxParallelism(64) },
            "forceNonParallel" to { forceNonParallel() },
            "setBufferTimeout" to { setBufferTimeout(10) },
            "disableChaining" to { disableChaining() },
            "startNewChain" to { startNewChain() },
            "slotSharingGroup" to { slotSharingGroup("texts") },
        )
        for ((setting, configure) in settings) {
            assertNotEquals(plain, configured(configure), setting)
        }
    }

    @Test
    fun `each forwarded repartitioning gives the edge the partitioner that the direct call gives it`() {
        val partitioner = Partitioner<Int> { key, partitions -> key % partitions }
        val cases: List<Triple<Class<*>, DataStream<String>.() -> DataStream<String>, FlinktDataStream<String>.() -> FlinktDataStream<String>>> =
            listOf(
                Triple(RebalancePartitioner::class.java, { rebalance() }, { rebalance() }),
                Triple(RescalePartitioner::class.java, { rescale() }, { rescale() }),
                Triple(ShufflePartitioner::class.java, { shuffle() }, { shuffle() }),
                Triple(ForwardPartitioner::class.java, { forward() }, { forward() }),
                Triple(GlobalPartitioner::class.java, { global() }, { global() }),
                Triple(CustomPartitionerWrapper::class.java, { partitionCustom(partitioner, byLength) }, { partitionCustom(partitioner, byLength) }),
            )
        for ((partitionerClass, onFlink, onView) in cases) {
            val direct = environment().apply { fromSequence(1, 100).map(toText, Types.STRING).onFlink().filter(nonEmpty) }
            val view = environment().apply { fromSequence(1, 100).flinkt().map(toText).onView().filter(nonEmpty) }

            val graph = structure(view)
            assertEquals(structure(direct), graph, partitionerClass.simpleName)
            assertEquals(listOf(StreamGraphs.Edge(2, partitionerClass, 0)), graph[1].edges, partitionerClass.simpleName)
        }
    }

    @Test
    fun `the forwarded assignTimestampsAndWatermarks adds the operator that the direct call adds`() {
        val strategy = WatermarkStrategy.forMonotonousTimestamps<String>()
            .withTimestampAssigner(SerializableTimestampAssigner { text, _ -> text.length.toLong() })
        val direct = environment().apply {
            fromSequence(1, 100).map(toText, Types.STRING).assignTimestampsAndWatermarks(strategy).name("timestamps").uid("timestamps-v1")
        }
        val view = environment().apply {
            fromSequence(1, 100).flinkt().map(toText).assignTimestampsAndWatermarks(strategy).name("timestamps").uid("timestamps-v1")
        }

        val graph = structure(view)
        assertEquals(structure(direct), graph)
        assertEquals(listOf("Source: Sequence Source", "Map", "timestamps"), graph.map { it.name })
        assertSame(BasicTypeInfo.STRING_TYPE_INFO, graph.last().outputType)
    }

    @Test
    fun `a sink added with the view's sinkTo is the sink that the direct call adds, in the same graph`() {
        val sink = DiscardingSink<String>()
        val direct = environment().apply {
            fromSequence(1, 100).map(toText, Types.STRING).name("texts").sinkTo(sink).name("discard").uid("discard-v1")
        }
        val view = environment().apply {
            fromSequence(1, 100).flinkt().map(toText).name("texts").sinkTo(sink).name("discard").uid("discard-v1")
        }

        assertEquals(listOf(sink), StreamGraphs.sinks(direct))
        assertEquals(listOf(sink), StreamGraphs.sinks(view))
        assertSame(sink, StreamGraphs.sinks(view).single())
        val graph = structure(view)
        assertEquals(structure(direct), graph)
        val (_, texts, writer) = graph
        assertEquals(listOf(StreamGraphs.Edge(2, ForwardPartitioner::class.java, 0)), texts.edges)
        assertEquals("discard-v1", writer.uid)
        assertEquals(listOf(StringSerializer.INSTANCE), writer.inputSerializers)
    }

    @Test
    fun `a side output taken with asFlink() and entered with flinkt() builds the graph that direct Flink calls build`() {
        val sink = DiscardingSink<String>()
        val direct = environment().apply {
            val evens = fromSequence(1, 100).process(split, Types.LONG).name("split").uid("split-v1")
            evens.getSideOutput(Split.ODD).filter(nonEmpty).name("odd").uid("odd-v1").sinkTo(sink).name("odd-sink")
        }
        val view = environment().apply {
            val evens = fromSequence(1, 100).process(split, Types.LONG).flinkt().name("split").uid("split-v1")
            evens.asFlink().getSideOutput(Split.ODD).flinkt().filter(nonEmpty).name("odd").uid("odd-v1").sinkTo(sink).name("odd-sink")
        }

        assertSame(sink, StreamGraphs.sinks(view).single())
        val graph = structure(view)
        assertEquals(structure(direct), graph)
        assertEquals(listOf("Source: Sequence Source", "split", "odd", "odd-sink: Writer"), graph.map { it.name })
        // The filter reads the side output, not the operator's main output.
        assertEquals(listOf(StreamGraphs.Edge(2, ForwardPartitioner::class.java, 0, Split.ODD.id)), graph[1].edges)
        assertEquals(listOf(StringSerializer.INSTANCE), graph[2].inputSerializers)
    }

    @Test
    fun `after forceNonParallel through the view, setParallelism and setMaxParallelism through it are rejected, as in Flink`() {
        val env = environment()
        val direct = env.fromSequence(1, 100).map(toText, Types.STRING).forceNonParallel()
        val view = env.fromSequence(1, 100).flinkt().map(toText).forceNonParallel()

        assertRejectedAsInFlink({ direct.setParallelism(2) }, { view.setParallelism(2) })
        assertRejectedAsInFlink({ direct.setMaxParallelism(2) }, { view.setMaxParallelism(2) })
        assertEquals(1, view.asFlink().parallelism)
        // The control: an operator that isn't non-parallel accepts both, through the view too.
        env.fromSequence(1, 100).flinkt().map(toText).setParallelism(2).setMaxParallelism(2)
    }

    @Test
    fun `a side output ID requested with another type after a fluent call through the view is rejected, as in Flink`() {
        val env = environment()
        val other = OutputTag(Split.ODD.id, Types.LONG)
        val direct = env.fromSequence(1, 100).process(Split(), Types.LONG)
        direct.getSideOutput(Split.ODD)
        val operator = env.fromSequence(1, 100).process(Split(), Types.LONG)
        operator.getSideOutput(Split.ODD)

        val left = operator.flinkt().name("split").asFlink()
        assertSame(operator, left)
        assertRejectedAsInFlink({ direct.name("split").getSideOutput(other) }, { left.getSideOutput(other) })
        // The control: the same ID with its own type is accepted.
        left.getSideOutput(Split.ODD)
    }

    @Test
    fun `setParallelism through the view of a non-parallel source is rejected, as in Flink`() {
        val env = environment()
        // A socket source reads one connection, so Flink builds it non-parallel. Building the graph doesn't connect.
        val direct = env.socketTextStream("localhost", 9)
        val source = env.socketTextStream("localhost", 9)

        assertRejectedAsInFlink({ direct.setParallelism(2) }, { source.flinkt().setParallelism(2) })
        // The control: a parallel source accepts it.
        assertEquals(2, env.fromSequence(1, 100).flinkt().setParallelism(2).asFlink().parallelism)
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
        assertSame(BasicTypeInfo.LONG_TYPE_INFO, process.keyType)
        assertEquals(LongSerializer.INSTANCE, process.keySerializer)
        assertSame(BasicTypeInfo.LONG_TYPE_INFO, process.outputType)
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

        // The same after a fluent call through the view, which wraps what Flink returned.
        val namedSource = env.fromSequence(1, 100)
        val namedOperator = env.fromSequence(1, 100).map(toText)
        val leftSource = namedSource.flinkt().name("numbers").setParallelism(2).asFlink()
        val leftOperator = namedOperator.flinkt().name("texts").uid("texts-v1").asFlink()
        assertSame(namedSource, leftSource)
        assertSame(namedOperator, leftOperator)
        leftSource.returns(longs)
        leftOperator.returns(texts)
        assertSame(longs, namedSource.type)
        assertSame(texts, namedOperator.type)

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

    /** Asserts that [direct], a call on a Flink object, and [view], the same call through a view, fail alike. */
    private fun assertRejectedAsInFlink(direct: () -> Any, view: () -> Any) {
        val expected = assertThrows<RuntimeException> { direct() }
        val actual = assertThrows<RuntimeException> { view() }
        assertEquals(expected.javaClass, actual.javaClass, "$actual")
        assertEquals(expected.message, actual.message)
    }

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

    /**
     * A `TypeInformation` for `String` that creates Flink's `StringSerializer`, as Flink's own does, and is equal only
     * to another of its class: what an operator that was given the wrong type could carry without changing its
     * serializer.
     */
    class OtherStringType : BasicTypeInfo<String>(
        String::class.java,
        arrayOf<Class<*>>(),
        StringSerializer.INSTANCE,
        StringComparator::class.java,
    ) {
        override fun canEqual(obj: Any?): Boolean = obj is OtherStringType

        override fun equals(other: Any?): Boolean = other is OtherStringType

        override fun hashCode(): Int = OtherStringType::class.java.hashCode()
    }

    private companion object {
        const val UID_HASH = "2e588ce1c86a9d46e2e85186773ce4fd"
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
