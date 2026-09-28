// SPIKE: graph equivalence and per-object state, composition view vs direct Flink vs subtype rebuild.
package probes.graph

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.streaming.api.transformations.PartitionTransformation
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import spike.compose.flinkt
import spike.model.Session
import spike.model.User
import spike.model.UserEvent
import spike.subtype.subEnterOperator
import spike.types.typeInfo

class SessionFunction : KeyedProcessFunction<Long, User, Session>() {
    override fun processElement(value: User, ctx: Context, out: Collector<Session>) = out.collect(Session(value.id, 1))
}

/** Structure of the transformation DAG ending at [t], without the global ids. */
fun describe(t: Transformation<*>): List<String> {
    val seen = LinkedHashMap<Int, String>()
    fun visit(x: Transformation<*>) {
        x.inputs.forEach(::visit)
        if (x.id in seen) return
        val out = runCatching { x.outputType.toString() }.getOrElse { "?" }
        val part = (x as? PartitionTransformation<*>)?.partitioner?.javaClass?.simpleName ?: ""
        val op = ((x as? OneInputTransformation<*, *>)?.operatorFactory as? SimpleOperatorFactory<*>)?.operator?.javaClass?.simpleName ?: ""
        seen[x.id] = "${x.javaClass.simpleName}[name=${x.name} uid=${x.uid} p=${x.parallelism} maxP=${x.maxParallelism} out=$out $part $op inputs=${x.inputs.size}]"
    }
    visit(t)
    return seen.values.toList()
}

fun env(): StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().apply { parallelism = 4 }
fun events(e: StreamExecutionEnvironment): DataStream<UserEvent> = e.fromData(listOf(UserEvent(1, "a")), typeInfo<UserEvent>())

fun check(name: String, ok: Boolean, detail: String = "") = println("| $name | ${if (ok) "PASS" else "FAIL"} | $detail |")

fun outcome(block: () -> Any?): String = runCatching { block(); "accepted" }.getOrElse { "rejected: ${it.javaClass.simpleName}: ${it.message?.take(90)}" }

fun main() {
    println("| check | result | detail |")
    println("|---|---|---|")
    // 1. First-milestone chain: view vs direct Flink given the TypeInformation the view should produce.
    run {
        val a = events(env()).flinkt()
            .map { User(it.userId, it.payload) }.name("users").uid("users-v1")
            .keyBy { it.id }
            .process(SessionFunction())
        val b = events(env())
            .map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>()).name("users").uid("users-v1")
            .keyBy(KeySelector<User, Long> { it.id }, Types.LONG)
            .process(SessionFunction(), typeInfo<Session>())   // Flink's @Internal overload, as the reference
        val da = describe(a.asFlink().transformation)
        val db = describe(b.transformation)
        check("milestone chain: same DAG as direct Flink (types, partitioner, uid, parallelism)", da == db, da.joinToString(" -> "))
    }
    // 2. Entering is identity: the view holds the exact object.
    run {
        val e = env()
        val op = events(e).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        val keyed = op.keyBy(KeySelector<User, Long> { it.id }, Types.LONG)
        val before = describe(keyed.transformation)
        check("op.flinkt().asFlink() === op", op.flinkt().asFlink() === op)
        check("keyed.flinkt().asFlink() === keyed", keyed.flinkt().asFlink() === keyed)
        check("entering adds no transformation", describe(keyed.flinkt().asFlink().transformation) == before)
    }
    // 3. forceNonParallel lives on the object.
    run {
        val direct = events(env()).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        direct.forceNonParallel()
        val flinkRes = outcome { direct.setParallelism(2) }
        val viaView = events(env()).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        viaView.forceNonParallel()
        val viewRes = outcome { viaView.flinkt().setParallelism(2) }
        val viaSub = events(env()).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        viaSub.forceNonParallel()
        val subRes = outcome { viaSub.subEnterOperator().setParallelism(2) }
        check("forceNonParallel then setParallelism(2): view behaves as Flink", flinkRes == viewRes, "flink: $flinkRes; view: $viewRes")
        check("forceNonParallel then setParallelism(2): subtype rebuild behaves as Flink", flinkRes == subRes, "subtype rebuild: $subRes")
    }
    // 4. Side outputs requested through the view are recorded on the original operator.
    run {
        val op = events(env()).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        op.flinkt().getSideOutput(OutputTag("late", Types.STRING))
        val res = outcome { op.getSideOutput(OutputTag("late", Types.INT)) }
        check("side output id reused with another type after view request: rejected as in Flink", res.startsWith("rejected"), res)
    }
    // 5. Non-parallel source: isParallel is a private field of DataStreamSource.
    run {
        val res = outcome { nonParallelSource(env()).flinkt().setParallelism(2) }
        check("non-parallel source: view setParallelism(2) rejected as in Flink", res.startsWith("rejected"), res)
    }
    // 6. reinterpretAsKeyedStream: the view keeps Flink's forward partitioning; a subtype rebuild adds a hash shuffle.
    run {
        val e1 = env()
        val base1 = events(e1).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        val re1 = DataStreamUtils.reinterpretAsKeyedStream(base1, KeySelector<User, Long> { it.id }, Types.LONG)
        val direct = describe(re1.process(SessionFunction(), typeInfo<Session>()).transformation)
        val e2 = env()
        val base2 = events(e2).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        val re2 = DataStreamUtils.reinterpretAsKeyedStream(base2, KeySelector<User, Long> { it.id }, Types.LONG)
        val view = describe(re2.flinkt().process(SessionFunction()).asFlink().transformation)
        val e3 = env()
        val base3 = events(e3).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        val re3 = DataStreamUtils.reinterpretAsKeyedStream(base3, KeySelector<User, Long> { it.id }, Types.LONG)
        val rebuilt = KeyedStream(re3, re3.keySelector, re3.keyType)   // what a subtype adapter can do with public constructors
        val sub = describe(rebuilt.process(SessionFunction(), typeInfo<Session>()).transformation)
        check("reinterpretAsKeyedStream: view DAG == direct", view == direct, direct.filter { "Partition" in it }.joinToString())
        check("reinterpretAsKeyedStream: subtype rebuild DAG == direct", sub == direct, sub.filter { "Partition" in it }.joinToString())
    }
    // 7. union through the view vs direct.
    run {
        val e1 = env(); val x1 = events(e1); val y1 = events(e1)
        val direct = describe(x1.union(y1).transformation)
        val e2 = env(); val x2 = events(e2); val y2 = events(e2)
        val view = describe(x2.flinkt().union(y2.flinkt()).asFlink().transformation)
        check("union through the view == direct", view == direct)
    }
    // 8. Side-output result enters as a view with the tag's type.
    run {
        val op = events(env()).map(MapFunction<UserEvent, User> { User(it.userId, it.payload) }, typeInfo<User>())
        val late = op.flinkt().getSideOutput(OutputTag("late", typeInfo<UserEvent>()))
        check("side output view carries the tag's TypeInformation", late.asFlink().type == typeInfo<UserEvent>(), late.asFlink().type.toString())
    }
    lineSpecific()
}
