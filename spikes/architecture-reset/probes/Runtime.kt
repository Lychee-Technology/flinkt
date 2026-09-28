// SPIKE: first-milestone job on a local MiniCluster with generic types disabled, so any Kryo use throws.
package probes.runtime

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.PipelineOptions
import org.apache.flink.core.memory.DataInputDeserializer
import org.apache.flink.core.memory.DataOutputSerializer
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import spike.compose.flinkt
import spike.model.Session
import spike.model.User
import spike.model.UserEvent
import spike.state.mapState
import spike.state.valueState
import spike.types.typeInfo

class CountingFunction : KeyedProcessFunction<Long, User, Session>() {
    private lateinit var last: ValueState<User>
    private lateinit var seen: MapState<String, Int>

    override fun open(openContext: OpenContext) {
        last = runtimeContext.valueState("last")
        seen = runtimeContext.mapState("seen")
    }

    override fun processElement(value: User, ctx: Context, out: Collector<Session>) {
        last.update(value)
        seen.put(value.name, (seen.get(value.name) ?: 0) + 1)
        out.collect(Session(value.id, seen.values().sum()))
    }
}

fun main() {
    val conf = Configuration().apply { set(PipelineOptions.GENERIC_TYPES, false) }
    val env = StreamExecutionEnvironment.getExecutionEnvironment(conf)
    env.parallelism = 2
    val events = env.fromData(
        listOf(UserEvent(1, "a"), UserEvent(2, "b"), UserEvent(1, "c"), UserEvent(1, "a")),
        typeInfo<UserEvent>(),
    )
    val sessions = events.flinkt()
        .map { User(it.userId, it.payload) }.name("users").uid("users-v1")
        .keyBy { it.id }
        .process(CountingFunction())
    val result = sessions.asFlink().executeAndCollect(10).sortedWith(compareBy({ it.userId }, { it.count }))
    println("MiniCluster result (generic types disabled): $result")

    // Serializer snapshot round trip through Flink's versioned snapshot format.
    val serializer = typeInfo<User>().createSerializer(SerializerConfigImpl())
    val bytes = DataOutputSerializer(64).also { serializer.serialize(User(7, "x"), it) }.copyOfBuffer
    val snap = DataOutputSerializer(64).also { TypeSerializerSnapshot.writeVersionedSnapshot(it, serializer.snapshotConfiguration()) }.copyOfBuffer
    val restored = TypeSerializerSnapshot.readVersionedSnapshot<User>(DataInputDeserializer(snap), Thread.currentThread().contextClassLoader)
    val compat = serializer.snapshotConfiguration().resolveSchemaCompatibility(restored)
    val value = restored.restoreSerializer().deserialize(DataInputDeserializer(bytes))
    println("snapshot restore: compatibility=${compat.isCompatibleAsIs} value=$value")
}
