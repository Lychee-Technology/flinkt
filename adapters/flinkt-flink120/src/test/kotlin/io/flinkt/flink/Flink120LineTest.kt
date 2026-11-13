package io.flinkt.flink

import io.flinkt.testing.compile.KotlinCompilation
import org.apache.flink.FlinkVersion
import org.apache.flink.api.common.ExecutionConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class Flink120LineTest {
    @Test
    fun `the adapter is built for 1_20`() {
        assertEquals(FlinkVersion.v1_20, ADAPTER_FLINK_LINE)
    }

    @Test
    fun `a 2_x-only Flink API doesn't compile on this adapter's classpath`() {
        KotlinCompilation()
            .compile(
                """
                import org.apache.flink.streaming.api.datastream.KeyedStream

                fun asyncState(stream: KeyedStream<String, String>) = stream.enableAsyncState()
                """,
            )
            .assertFailure("UNRESOLVED_REFERENCE", "Unresolved reference 'enableAsyncState'")
    }

    @Test
    @Suppress("DEPRECATION")
    fun `1_20's ExecutionConfig serializer factory creates Flink's own serializer for each built-in`() {
        // Only 1.20 has it: the abstract factory there, removed in 2.x. BasicTypeInfo implements both factories.
        val config = ExecutionConfig()
        for (case in TypeInfoTest.builtIns) {
            assertEquals(case.serializer, case.typeInfo.createSerializer(config).javaClass, case.kind.kotlinName)
        }
    }
}
