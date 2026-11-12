package io.flinkt.flink

import io.flinkt.testing.compile.KotlinCompilation
import org.apache.flink.FlinkVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class Flink23LineTest {
    @Test
    fun `the adapter is built for 2_3`() {
        assertEquals(FlinkVersion.v2_3, ADAPTER_FLINK_LINE)
    }

    @Test
    fun `a 2_x-only Flink API compiles on this adapter's classpath`() {
        KotlinCompilation()
            .compile(
                """
                import org.apache.flink.streaming.api.datastream.KeyedStream

                fun asyncState(stream: KeyedStream<String, String>) = stream.enableAsyncState()
                """,
            )
            .assertSuccess()
    }
}
