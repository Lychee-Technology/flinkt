// SPIKE: exact-type assertion forms, ResultTypeQueryable on raw Flink calls, explicit nullable result for a Java function.
package probes.extra

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import spike.compose.FlinktOperator
import spike.compose.flinkt
import spike.java.JavaToLength
import spike.model.User
import spike.subtype.subEnter
import spike.types.typeInfo
import kotlin.reflect.typeOf

class Exactly<T>
fun <T> exactTypeOf(@Suppress("UNUSED_PARAMETER") value: T): Exactly<T> = Exactly()
fun <U> Exactly<U>.shouldBe() = Unit
inline fun <reified T> staticTypeOf(@Suppress("UNUSED_PARAMETER") value: T) = typeOf<T>()

class TypedToUser : MapFunction<String, User>, ResultTypeQueryable<User> {
    override fun map(value: String) = User(1, value)
    override fun getProducedType(): TypeInformation<User> = typeInfo()
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val raw: DataStream<String> = env.fromData(listOf("a"), Types.STRING)
    val s = raw.flinkt()
    val mapped = s.map { User(1, it) }
    exactTypeOf(mapped).shouldBe<FlinktOperator<User>>()       // single-expression exact check, lambda result is User!
    val sub = raw.subEnter()
    exactTypeOf(sub.union(sub)).shouldBe<DataStream<String>>()
    println("KType equality on SAM-lambda result: ${staticTypeOf(mapped) == typeOf<FlinktOperator<User>>()} (${staticTypeOf(mapped)})")
    println("raw Flink map(fn implementing ResultTypeQueryable): ${raw.map(TypedToUser()).type}")
    println("explicit nullable result for a Java function: ${runCatching { s.map<Int?>(JavaToLength()).asFlink().type }.exceptionOrNull()?.message}")
}
