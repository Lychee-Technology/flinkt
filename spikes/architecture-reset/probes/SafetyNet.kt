// SPIKE: does pipeline.generic-types=false catch Kryo introduced by a raw Flink call after asFlink()?
package probes.safety

import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.PipelineOptions
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import spike.compose.flinkt
import spike.model.User
import spike.model.UserEvent
import spike.types.typeInfo

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment(Configuration().apply { set(PipelineOptions.GENERIC_TYPES, false) })
    val events = env.fromData(listOf(UserEvent(1, "a")), typeInfo<UserEvent>())
    val raw = events.flinkt().asFlink().map { User(it.userId, it.payload) }   // explicit exit, then Flink's own map
    println("raw map output type: ${raw.type}")
    val r = runCatching { raw.executeAndCollect(10) }
    println("execute with generic types disabled: ${r.exceptionOrNull()?.let { generateSequence(it) { e -> e.cause }.last().let { c -> "${c.javaClass.simpleName}: ${c.message?.take(160)}" } } ?: "succeeded"}")
}
