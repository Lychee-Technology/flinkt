package io.flinkt.flink.format

import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.types.StringValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The record format's evidence on a Flink release that has no adapter yet, 2.2.1 until #27 adds one
 * (docs/persisted-format.md, "Evidence"): `RecordFormatTest` and `StringFormatTest` compiled and run against it. This
 * test makes sure they did run on it, so the evidence can't silently fall back to another release.
 */
class FormatFlinkLineTest {
    @Test
    fun `the format tests run on the release the build names`() {
        val expected = checkNotNull(System.getProperty("flinkt.test.formatFlinkVersion")) {
            "the build passes the Flink release these tests run on as flinkt.test.formatFlinkVersion"
        }
        assertEquals(expected, EnvironmentInformation.getVersion())
        val jar = StringValue::class.java.protectionDomain.codeSource.location.path
        assertTrue(jar.endsWith("flink-core-$expected.jar"), jar)
    }
}
