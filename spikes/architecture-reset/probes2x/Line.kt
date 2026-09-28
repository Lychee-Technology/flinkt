package probes.graph

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.KeyedStream
import spike.compose.flinkt
import spike.model.Session
import spike.model.User
import spike.types.typeInfo

// Flink 2.x: KeyedStream.enableAsyncState() sets a private field that keyed operators read.
fun lineSpecific() {
    val e1 = env()
    val k1 = events(e1).map(MapFunction { User(it.userId, it.payload) }, typeInfo<User>()).keyBy(KeySelector<User, Long> { it.id }, Types.LONG)
    k1.enableAsyncState()
    val direct = describe(k1.process(SessionFunction(), typeInfo<Session>()).transformation).last()
    val e2 = env()
    val k2 = events(e2).map(MapFunction { User(it.userId, it.payload) }, typeInfo<User>()).keyBy(KeySelector<User, Long> { it.id }, Types.LONG)
    k2.enableAsyncState()
    val view = describe(k2.flinkt().process(SessionFunction()).asFlink().transformation).last()
    val e3 = env()
    val k3 = events(e3).map(MapFunction { User(it.userId, it.payload) }, typeInfo<User>()).keyBy(KeySelector<User, Long> { it.id }, Types.LONG)
    k3.enableAsyncState()
    val rebuilt = KeyedStream(k3, k3.keySelector, k3.keyType)
    val sub = describe(rebuilt.process(SessionFunction(), typeInfo<Session>()).transformation).last()
    check("2.x enableAsyncState then process: view operator == direct", view == direct, direct)
    check("2.x enableAsyncState then process: subtype rebuild operator == direct", sub == direct, sub)
}

fun nonParallelSource(e: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment) =
    e.addSource(object : org.apache.flink.streaming.api.functions.source.legacy.SourceFunction<Long> {
        override fun run(ctx: org.apache.flink.streaming.api.functions.source.legacy.SourceFunction.SourceContext<Long>) {}
        override fun cancel() {}
    }, Types.LONG)
