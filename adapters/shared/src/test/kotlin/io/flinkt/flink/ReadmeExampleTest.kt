package io.flinkt.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

// These two functions are the comparison in the root README.
fun direct(stream: DataStream<Long>): SingleOutputStreamOperator<String> =
    stream.map(MapFunction<Long, String> { it.toString() }, Types.STRING)

fun withFlinkt(stream: DataStream<Long>): SingleOutputStreamOperator<String> =
    stream.flinkt().map { it.toString() }.asFlink()

class ReadmeExampleTest {
    @Test
    fun `both examples give Flink its String type information`() {
        val env = StreamExecutionEnvironment.createLocalEnvironment(2)
        val source = env.fromSequence(1, 3)

        assertSame(source, source.flinkt().asFlink())
        assertSame(BasicTypeInfo.STRING_TYPE_INFO, direct(source).type)
        assertSame(BasicTypeInfo.STRING_TYPE_INFO, withFlinkt(source).type)
    }
}
