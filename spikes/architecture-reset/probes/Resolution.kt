// SPIKE: which overload each call resolves to, its exact static type, and the TypeInformation Flink receives.
package probes

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import spike.compose.FlinktStream
import spike.compose.flinkt
import spike.java.JavaToLength
import spike.model.Session
import spike.model.User
import spike.subtype.subEnter
import spike.types.staticType

class ToUser : MapFunction<String, User> {
    override fun map(value: String) = User(value.length.toLong(), value)
}

class ToUserRich : RichMapFunction<String, User>() {
    override fun map(value: String) = User(value.length.toLong(), value)
}

class SessionFunction : KeyedProcessFunction<Long, User, Session>() {
    override fun processElement(value: User, ctx: Context, out: Collector<Session>) = out.collect(Session(value.id, 1))
}

class MaybeUser : ProcessFunction<String, User?>() {
    override fun processElement(value: String, ctx: Context, out: Collector<User?>) = out.collect(null)
}

class Passthrough<X> : ProcessFunction<X, X>() {
    override fun processElement(value: X, ctx: Context, out: Collector<X>) = out.collect(value)
}

fun <T> expectType(@Suppress("UNUSED_PARAMETER") value: T) {}

fun row(case: String, static: () -> String, type: () -> Any?) {
    val st = runCatching(static).getOrElse { "THREW ${it.javaClass.simpleName}: ${it.message}" }
    val ty = runCatching { type().toString() }.getOrElse { "THREW ${it.javaClass.simpleName}: ${it.message?.take(160)}" }
    println("| $case | `$st` | $ty |")
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val raw: DataStream<String> = env.fromData(listOf("a", "bb"), Types.STRING)
    val s: FlinktStream<String> = raw.flinkt()
    println("| case | exact static type | output TypeInformation Flink holds |")
    println("|---|---|---|")
    // Composition view: one reified overload per operator, taking Flink's own function type.
    row("compose: s.map { User(...) }", { staticType(s.map { User(it.length.toLong(), it) }) }) { s.map { User(it.length.toLong(), it) }.asFlink().type }
    row("compose: s.map(MapFunction<String, User> { ... })", { staticType(s.map(MapFunction<String, User> { User(1, it) })) }) { s.map(MapFunction<String, User> { User(1, it) }).asFlink().type }
    row("compose: s.map(ToUser())", { staticType(s.map(ToUser())) }) { s.map(ToUser()).asFlink().type }
    row("compose: s.map(ToUserRich())", { staticType(s.map(ToUserRich())) }) { s.map(ToUserRich()).asFlink().type }
    row("compose: s.map(JavaToLength())", { staticType(s.map(JavaToLength())) }) { s.map(JavaToLength()).asFlink().type }
    row("compose: s.map { it.length }", { staticType(s.map { it.length }) }) { s.map { it.length }.asFlink().type }
    row("compose: s.map { it.takeIf { false } } (String?)", { "n/a" }) { s.map { it.takeIf { false } }.asFlink().type }
    row("compose: s.process(MaybeUser()) (User?)", { "n/a" }) { s.process(MaybeUser()).asFlink().type }
    row("compose: s.process(Passthrough())", { staticType(s.process(Passthrough())) }) { s.process(Passthrough()).asFlink().type }
    val keyed = s.map(ToUser()).keyBy { it.id }
    row("compose: map(ToUser()).keyBy { it.id }", { staticType(keyed) }) { keyed.asFlink().keyType }
    row("compose: keyed.process(SessionFunction())", { staticType(keyed.process(SessionFunction())) }) { keyed.process(SessionFunction()).asFlink().type }
    row("compose: keyBy(KeySelector<User, Long> { it.id })", { staticType(s.map(ToUser()).keyBy(KeySelector<User, Long> { it.id })) }) { s.map(ToUser()).keyBy(KeySelector<User, Long> { it.id }).asFlink().keyType }
    row("compose: s.flatMap<Int> { v, out -> out.collect(v.length) }", { staticType(s.flatMap<Int> { v, out -> out.collect(v.length) }) }) { s.flatMap<Int> { v, out -> out.collect(v.length) }.asFlink().type }
    row("compose: s.union(s, s)", { staticType(s.union(s, s)) }) { s.union(s, s).asFlink().type }
    row("compose: s.map { ... }.name(\"n\").uid(\"u\").setParallelism(2)", { staticType(s.map(ToUser()).name("n").uid("u").setParallelism(2)) }) { "-" }
    row("compose: s.asFlink().map { User(...) } (explicit exit)", { staticType(s.asFlink().map { User(it.length.toLong(), it) }) }) { s.asFlink().map { User(it.length.toLong(), it) }.type }
    row("compose: raw.map(ToUser()).flinkt()", { staticType(raw.map(ToUser()).flinkt()) }) { "-" }
    row("compose: keyedFlink.flinkt()", { staticType(raw.keyBy { it }.flinkt()) }) { "-" }

    // Subtype façade (PR #1): lambda overload vs Flink's inherited members.
    val sub = raw.subEnter()
    row("subtype: sub.map { User(...) }", { staticType(sub.map { User(it.length.toLong(), it) }) }) { sub.map { User(it.length.toLong(), it) }.type }
    row("subtype: sub.map(MapFunction<String, User> { ... })", { staticType(sub.map(MapFunction<String, User> { User(1, it) })) }) { sub.map(MapFunction<String, User> { User(1, it) }).type }
    row("subtype: sub.map(ToUser())", { staticType(sub.map(ToUser())) }) { sub.map(ToUser()).type }
    row("subtype: sub.union(sub)", { staticType(sub.union(sub)) }) { sub.union(sub).type }
    row("subtype: sub.keyBy { it }", { staticType(sub.keyBy { it }) }) { sub.keyBy { it }.keyType }

    // Exact-type assertions.
    expectType<DataStream<String>>(sub) // compiles although sub is a SubStream: supertype assignability, not exactness
    println("expectType<DataStream<String>>(sub) compiled; staticType(sub) = ${staticType(sub)}; staticType(sub.union(sub)) = ${staticType(sub.union(sub))}")
    val flinkOp: SingleOutputStreamOperator<User> = s.map(ToUser()).asFlink()
    println("identity: s.map(...).asFlink() is Flink's own object: ${flinkOp.javaClass.name}")
}
