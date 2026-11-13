package io.flinkt.flink

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

/** The views on what only Flink 1.20 has, or lacks. The shared view tests cover the rest. */
class ViewLineTest {
    @Test
    fun `1_20's stability annotations are in the class files only, where the shared stability check reads them`() {
        val process = KeyedStream::class.java.getMethod("process", KeyedProcessFunction::class.java)
        // Reflection doesn't see a class-retention annotation.
        assertNull(process.getAnnotation(PublicEvolving::class.java))
        val call = ClassFileReader.Call(
            "org/apache/flink/streaming/api/datastream/KeyedStream",
            "process",
            org.objectweb.asm.Type.getMethodDescriptor(process),
        )
        val (_, method) = ClassFileReader.resolve(call)
        assertEquals(setOf("Lorg/apache/flink/annotation/PublicEvolving;"), ClassFileReader.annotations(method))
    }
}
