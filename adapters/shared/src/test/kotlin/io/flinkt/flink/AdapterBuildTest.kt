package io.flinkt.flink

import io.flinkt.testing.ClassFiles
import org.apache.flink.FlinkVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Runs on every adapter lane: the shared source compiled against the lane's Flink, and tests on that Flink. */
class AdapterBuildTest {
    @Test
    fun `tests run on the adapter's own Flink line`() {
        assertEquals(ADAPTER_FLINK_LINE, FlinkVersion.current())
        assertTrue(isAdapterLine())
    }

    @Test
    fun `shared and per-line classes target Java 8 and carry Kotlin metadata 2_4_0`() {
        for (name in listOf("io.flinkt.flink.AdapterLineKt", "io.flinkt.flink.FlinkLineKt", "io.flinkt.flink.TypeInfoKt")) {
            val type = Class.forName(name)
            assertEquals(52, ClassFiles.majorVersion(type), name)
            assertEquals("2.4.0", ClassFiles.kotlinMetadataVersion(type), name)
        }
    }
}
