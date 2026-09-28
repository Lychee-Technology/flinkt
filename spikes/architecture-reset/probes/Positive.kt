// SPIKE: positive compile probes whose inferred types matter.
package probes.positive

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import spike.compose.flinkt
import spike.model.User
import spike.state.mapState
import spike.state.valueState
import spike.state.valueStateDescriptor
import spike.subtype.subEnter
import spike.types.staticType

// README state example, redesigned API: binding helpers return bound state, inferred from the property type.
class UserFunction : RichMapFunction<String, User>() {
    private lateinit var users: MapState<Long, User>
    private lateinit var last: ValueState<User>

    override fun open(openContext: OpenContext) {
        users = runtimeContext.mapState("users")
        last = runtimeContext.valueState("last")
    }

    override fun map(value: String): User = User(value.length.toLong(), value)
}

class Exactly<T>
fun <T> exactTypeOf(@Suppress("UNUSED_PARAMETER") value: T): Exactly<T> = Exactly()

// Extension-only façade: can a same-name extension ever win against Flink's member?
fun <T, R> DataStream<T>.map(kotlinFn: (T) -> R): String = "extension"

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val raw: DataStream<String> = env.fromData(listOf("a", "bb"), Types.STRING)
    val s = raw.flinkt()
    val fm = s.flatMap { v, out -> out.collect(v.length) }
    println("flatMap without type argument: static ${staticType(fm)}; Flink type ${fm.asFlink().type}")
    val d = valueStateDescriptor<User>("user")
    println("valueStateDescriptor<User>: ${staticType(d)}; serializer type ${d.type}")
    val sub = raw.subEnter()
    val ok: Exactly<DataStream<String>> = exactTypeOf(sub.union(sub)).let { it } // positive half: union's result is exactly DataStream
    println("exactTypeOf(sub.union(sub)) accepted as Exactly<DataStream<String>>: ${ok.javaClass.simpleName}")
    println("ext: raw.map { it.length } resolves to: ${staticType(raw.map { it.length })}")
    println("ext: raw.map(kotlinFn = { it.length }) resolves to: ${staticType(raw.map(kotlinFn = { it.length }))}")
}
