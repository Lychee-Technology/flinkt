package io.flinkt.codegen

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class FlinkApiTest {
    private val union = FlinkMethod(DATA_STREAM, "union", "([L$DATA_STREAM_PATH;)L$DATA_STREAM_PATH;", listOf("streams"))
    private val rebalance = FlinkMethod(DATA_STREAM, "rebalance", "()L$DATA_STREAM_PATH;", emptyList())
    private val executeAndCollect = FlinkMethod(DATA_STREAM, "executeAndCollect", "(Ljava/lang/String;I)Ljava/util/List;", listOf("jobExecutionName", "limit"))
    private val api = FlinkApi("2.3.0", listOf(union, rebalance, executeAndCollect))

    @Test
    fun `the file records the version and one sorted row per method`() {
        val rows = api.write().lines().filter { it.isNotEmpty() && !it.startsWith('#') }
        assertEquals(
            listOf(
                "flink-version\t2.3.0",
                "$DATA_STREAM\texecuteAndCollect\tjobExecutionName,limit\t(Ljava/lang/String;I)Ljava/util/List;",
                "$DATA_STREAM\trebalance\t\t()L$DATA_STREAM_PATH;",
                "$DATA_STREAM\tunion\tstreams\t([L$DATA_STREAM_PATH;)L$DATA_STREAM_PATH;",
            ),
            rows,
        )
        assertTrue(api.write().lines().none { it.isNotEmpty() && it.last().isWhitespace() }, "no row ends in whitespace")
    }

    @Test
    fun `a written file parses back`() {
        assertEquals(api, FlinkApi.parse(api.write(), "flink-api.tsv"))
    }

    @Test
    fun `a method is found by its declaring class, name and descriptor`() {
        assertEquals(listOf("streams"), api.find(DATA_STREAM, "union", union.descriptor)?.parameterNames)
        assertNull(api.find(DATA_STREAM, "union", "(L$DATA_STREAM_PATH;)L$DATA_STREAM_PATH;"))
        assertNull(api.find("$DATA_STREAM\$Collector", "union", union.descriptor))
    }

    @Test
    fun `a malformed file fails, naming the line`() {
        val cases = mapOf(
            "$DATA_STREAM\tunion\tstreams\t([L$DATA_STREAM_PATH;)V\n" to "f.tsv:1: the first row must be 'flink-version<tab><version>'",
            "flink-version\t2.3.0\n$DATA_STREAM\tunion\tstreams\n" to "f.tsv:2: a row has 4 tab-separated columns",
            "flink-version\t2.3.0\n$DATA_STREAM\tunion\t\t([L$DATA_STREAM_PATH;)V\n" to "f.tsv:2: $DATA_STREAM.union([L$DATA_STREAM_PATH;)V has 1 parameters, but 0 names",
            "flink-version\t2.3.0\n$DATA_STREAM\tunion\tstreams\t(V\n" to "f.tsv:2: Not a method descriptor",
            "# comment only\n" to "f.tsv has no 'flink-version' row",
            "flink-version\t2.3.0\n$DATA_STREAM\tm\t\t()V\n$DATA_STREAM\tm\t\t()V\n" to "f.tsv: A method is listed twice: $DATA_STREAM#m()V",
        )
        for ((text, message) in cases) {
            val failure = assertThrows<IllegalArgumentException>(text) { FlinkApi.parse(text, "f.tsv") }
            assertTrue(failure.message!!.startsWith(message), "'${failure.message}' should start with '$message'")
        }
    }

    private companion object {
        const val DATA_STREAM = "org.apache.flink.streaming.api.datastream.DataStream"
        const val DATA_STREAM_PATH = "org/apache/flink/streaming/api/datastream/DataStream"
    }
}
