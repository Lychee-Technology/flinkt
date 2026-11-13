package io.flinkt.flink

import io.flinkt.flink.ViewCodegen.generated
import io.flinkt.flink.ViewCodegen.job
import io.flinkt.flink.ViewCodegen.jobWarnings
import io.flinkt.flink.ViewCodegen.views
import io.flinkt.testing.compile.KotlinCompiler
import org.apache.flink.annotation.Internal
import org.apache.flink.streaming.api.datastream.DataStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/** The generator on what only Flink 1.20 has, or lacks. The shared `ViewCodegenTest` covers the rest. */
class ViewCodegenLineTest {
    @Test
    fun `a method that only 2_x has fails the build on 1_20`() {
        ViewCodegen.compile(views(operator = listOf("enableAsyncState")))
            .assertFailure("Operator forwards enableAsyncState, but SingleOutputStreamOperator has no public method enableAsyncState in Flink 1.20.5")
    }

    @Test
    fun `1_20's stability annotations have class retention, which the shared @Internal refusal reads`() {
        // Reflection doesn't see a class-retention annotation, so the shared refusal of getTransformation, which
        // passes on this line, shows that the generator reads them from the class file.
        assertNull(DataStream::class.java.getMethod("getTransformation").getAnnotation(Internal::class.java))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `1_20's deprecated partitionCustom overloads warn, and the one that isn't deprecated doesn't`(compiler: KotlinCompiler) {
        val result = ViewCodegen.compile(
            views(stream = listOf("partitionCustom")),
            job(
                """
                import org.apache.flink.api.common.functions.Partitioner

                fun job(events: Stream<String>, partitioner: Partitioner<Int>) {
                    events.partitionCustom(partitioner, { it.length })
                    events.partitionCustom(partitioner, field = 0)
                }
                """,
            ),
            compiler = compiler,
        ).assertWarning("Deprecated in Flink: DataStream.partitionCustom")
        assertEquals(listOf(7), result.jobWarnings.map { it.line }, result.jobWarnings.toString())
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `1_20's sinkTo for the legacy Sink is forwarded unannotated, though the Sink is deprecated`(compiler: KotlinCompiler) {
        val result = ViewCodegen.compile(
            views(stream = listOf("sinkTo")),
            job(
                """
                @Suppress("DEPRECATION")
                fun legacySink(): org.apache.flink.api.connector.sink.Sink<String, *, *, *> = TODO()

                fun job(events: Stream<String>) = events.sinkTo(legacySink())
                """,
            ),
            compiler = compiler,
        ).assertSuccess()
        assertEquals(emptyList<Any>(), result.jobWarnings)
        assertTrue("@Deprecated" !in result.generated("Stream"))
    }
}
