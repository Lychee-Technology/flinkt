// SPIKE: the README's code, compiled against DocsApi.kt. Types are stand-ins where the README elides them.
package flinkt.docs.snippets

import flinkt.docs.FlinktDataStream
import flinkt.docs.FlinktKeyedStream
import flinkt.docs.FlinktSingleOutputStreamOperator
import flinkt.docs.flinkt
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.source.Source
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import spike.model.Session
import spike.model.User
import spike.model.UserEvent
import spike.state.mapState
import spike.state.valueState
import spike.types.typeInfo

class SessionFunction : KeyedProcessFunction<Long, UserEvent, Session>() {
    override fun processElement(value: UserEvent, ctx: Context, out: Collector<Session>) = out.collect(Session(value.userId, 1))
}

fun readmeFirstExample(
    env: StreamExecutionEnvironment,
    source: Source<UserEvent, *, *>,
    watermarkStrategy: WatermarkStrategy<UserEvent>,
    sink: Sink<Session>,
) {
    val sessions =
        env.fromSource(source, watermarkStrategy, "events")
            .flinkt()
            .filter { it.payload.isNotBlank() }
            .map { UserEvent(userId = it.userId, payload = it.payload.trim()) }
            .name("normalize")
            .uid("normalize-v1")
            .keyBy { it.userId }
            .process(SessionFunction())
    sessions.asFlink().sinkTo(sink)
}

fun viewIdentity(parsed: SingleOutputStreamOperator<UserEvent>) {
    val events = parsed.flinkt()
    val typed: FlinktSingleOutputStreamOperator<UserEvent> = events
    check(typed.asFlink() === parsed)
}

class VisitCount(val n: Long)
class UserVisits(val id: Long, val n: Long)
class CountVisits : AggregateFunction<User, VisitCount, UserVisits> {
    override fun createAccumulator() = VisitCount(0)
    override fun add(value: User, acc: VisitCount) = VisitCount(acc.n + 1)
    override fun getResult(acc: VisitCount) = UserVisits(0, acc.n)
    override fun merge(a: VisitCount, b: VisitCount) = VisitCount(a.n + b.n)
}

// typeInfo<VisitCount>() would fail at runtime in the spike (not a modeled type); this only has to compile.
fun leavingTheView(users: FlinktKeyedStream<User, Long>, windowAssigner: WindowAssigner<Any, TimeWindow>) =
    users
        .asFlink()
        .window(windowAssigner)
        .aggregate(CountVisits(), typeInfo<VisitCount>(), typeInfo<UserVisits>())
        .flinkt()
        .name("visits")

fun <R> enrich(
    events: FlinktDataStream<UserEvent>,
    fn: MapFunction<UserEvent, R>,
    type: TypeInformation<R>,
) = events.map(fn, type)

class UserFunction : RichMapFunction<UserEvent, User>() {
    private lateinit var users: MapState<Long, User>
    private lateinit var user: ValueState<User>

    override fun open(openContext: OpenContext) {
        users = runtimeContext.mapState("users")
        user = runtimeContext.valueState<User>("user")
    }

    override fun map(value: UserEvent): User = User(value.userId, value.payload)
}

class UserSessionFunction : KeyedProcessFunction<Long, User, Session>() {
    private lateinit var user: ValueState<User>
    override fun open(openContext: OpenContext) {
        user = runtimeContext.valueState<User>("user")
    }
    override fun processElement(value: User, ctx: Context, out: Collector<Session>) = out.collect(Session(value.id, 1))
}

fun milestone(events: SingleOutputStreamOperator<UserEvent>) =
    events
        .flinkt()
        .map { User(id = it.userId, name = it.payload) }
        .name("users")
        .keyBy { it.id }
        .process(UserSessionFunction())
