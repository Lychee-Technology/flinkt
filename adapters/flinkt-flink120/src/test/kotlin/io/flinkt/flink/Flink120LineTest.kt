package io.flinkt.flink

import io.flinkt.testing.compile.KotlinCompilation
import org.apache.flink.FlinkVersion
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
}
