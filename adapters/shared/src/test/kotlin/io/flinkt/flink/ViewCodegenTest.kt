package io.flinkt.flink

import io.flinkt.flink.ViewCodegen.editedFlinkApiFile
import io.flinkt.flink.ViewCodegen.flinkVersion
import io.flinkt.flink.ViewCodegen.generated
import io.flinkt.flink.ViewCodegen.job
import io.flinkt.flink.ViewCodegen.jobWarnings
import io.flinkt.flink.ViewCodegen.refusals
import io.flinkt.flink.ViewCodegen.views
import io.flinkt.testing.compile.CompilationResult
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.SourceFile
import java.io.File
import org.apache.flink.streaming.api.datastream.DataStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * `flinkt-view-codegen` against this adapter's Flink line (docs/testing.md#generated-forwarders). Every adapter lane
 * runs these, because each line's classes differ. The tests that only hold on one line are in that line's
 * `ViewCodegenLineTest`.
 *
 * A refusal is a KSP error, which doesn't depend on the Kotlin compiler, so those tests compile with the build
 * compiler. What a caller of a forwarder sees, a warning or a type, is the compiler's, so those tests run on both.
 */
class ViewCodegenTest {
    @TempDir
    lateinit var dir: File

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `forwarders are members of the view, with Flink's overloads and parameter names`(compiler: KotlinCompiler) {
        val result = ViewCodegen.compile(
            views(
                stream = listOf("filter", "union", "rebalance", "partitionCustom", "print", "executeAndCollect"),
                operator = listOf("name", "uid", "setParallelism", "setBufferTimeout"),
                keyed = listOf("reduce"),
            ),
            job(
                """
                import io.flinkt.testing.exactTypeOf
                import io.flinkt.testing.shouldBe
                import org.apache.flink.api.common.functions.Partitioner
                import org.apache.flink.streaming.api.datastream.DataStreamSink

                fun job(events: Stream<String>, other: Stream<String>, keyed: Keyed<String, Int>): Operator<String> {
                    // union(DataStream<T>... streams) takes views, a keyed one included.
                    exactTypeOf(events.union(other, keyed)).shouldBe<Stream<String>>()
                    exactTypeOf(events.rebalance()).shouldBe<Stream<String>>()
                    // A method type parameter that the result doesn't use is kept.
                    exactTypeOf(events.partitionCustom(Partitioner<Int> { key, n -> key % n }, { it.length })).shouldBe<Stream<String>>()
                    exactTypeOf(events.print(sinkIdentifier = "events")).shouldBe<DataStreamSink<String>>()
                    // A returned java.util.List is Kotlin's read-only List.
                    exactTypeOf(events.executeAndCollect(limit = 3)).shouldBe<List<String>>()
                    exactTypeOf(events.executeAndCollect(jobExecutionName = "job", limit = 3)).shouldBe<List<String>>()
                    exactTypeOf(keyed.reduce { a, b -> a + b }).shouldBe<Operator<String>>()
                    val filtered = events.filter { it.isNotEmpty() }
                    exactTypeOf(filtered).shouldBe<Operator<String>>()
                    return filtered.name("filter").uid("filter-v1").setParallelism(parallelism = 2).setBufferTimeout(timeoutMillis = 10)
                }
                """,
            ),
            compiler = compiler,
        ).assertSuccess()
        assertEquals(emptyList<Any>(), result.jobWarnings)
        assertTrue("public fun union(vararg streams: sample.Stream<T>): sample.Stream<T>" in result.generated("Stream"))
    }

    @Test
    fun `a forwarder makes one call on the Flink object and wraps what Flink returns`() {
        val result = ViewCodegen.compile(
            views(stream = listOf("filter", "union"), operator = listOf("name", "setParallelism", "forceNonParallel")),
            job(
                """
                import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

                fun configure(numbers: SingleOutputStreamOperator<Long>): List<Any> {
                    val view = Operator(numbers)
                    val named = view.name("numbers")
                    val filtered = view.filter { it > 2 }
                    val serial = filtered.forceNonParallel()
                    // Flink's own check applies: a non-parallel operator refuses another parallelism.
                    val refused = runCatching { serial.setParallelism(2) }.exceptionOrNull()
                    return listOf(named.asFlink(), filtered.asFlink(), serial.asFlink(), view.union(filtered).asFlink(), refused ?: "none")
                }
                """,
            ),
        ).assertSuccess()
        val numbers = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment().fromSequence(1, 10)
        @Suppress("UNCHECKED_CAST")
        val (named, filtered, serial, union, refused) = result.classLoader().loadClass("sample.JobKt")
            .getMethod("configure", org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator::class.java)
            .invoke(null, numbers) as List<Any>
        // name() returns the operator itself, and the view wraps that object rather than building another.
        assertTrue(named === numbers)
        assertEquals("numbers", numbers.name)
        assertTrue(filtered !== numbers)
        assertTrue(serial === filtered)
        assertEquals(1, (filtered as DataStream<*>).parallelism)
        assertTrue(union is DataStream<*> && union !== numbers)
        assertTrue(refused is IllegalArgumentException, "setParallelism after forceNonParallel: $refused")
    }

    @Test
    fun `a method missing on the line fails the build, naming it and the Flink version`() {
        ViewCodegen.compile(views(stream = listOf("filter", "noSuchMethod")))
            .assertFailure("Stream forwards noSuchMethod, but DataStream has no public method noSuchMethod in Flink $flinkVersion")
    }

    @Test
    fun `a type-introducing method fails the build, for each overload`() {
        ViewCodegen.compile(views(stream = listOf("map", "connect")))
            .assertFailure(
                "DataStream.map(MapFunction<T, R>) returns SingleOutputStreamOperator<R>, a type built from its type parameter",
                "DataStream.map(MapFunction<T, R>, TypeInformation<R>) returns SingleOutputStreamOperator<R>",
                "DataStream.connect(DataStream<R>) returns ConnectedStreams<T, R>",
                "DataStream.connect(BroadcastStream<R>) returns BroadcastConnectedStream<T, R>",
                "Write it by hand, with a reified type parameter",
            )
    }

    @Test
    fun `a method returning a stream type without a view fails the build, though another overload passes`() {
        val result = ViewCodegen.compile(views(stream = listOf("broadcast")))
            .assertFailure(
                "DataStream.broadcast(MapStateDescriptor<*, *>...) returns BroadcastStream<T>, and BroadcastStream is a " +
                    "Flink stream type that has no view: forwarding it would let a chain leave the view without asFlink()",
            )
        // broadcast() returns a DataStream, which has a view, so only the other overload fails.
        assertEquals(1, result.refusals.size, result.refusals.toString())
    }

    @Test
    fun `an @Internal method fails the build`() {
        ViewCodegen.compile(views(stream = listOf("getTransformation")))
            .assertFailure("DataStream.getTransformation() is @Internal in Flink $flinkVersion, so it isn't public API")
    }

    @Test
    fun `every refusal of a build is reported`() {
        val result = ViewCodegen.compile(views(stream = listOf("noSuchMethod", "map", "getTransformation"), operator = listOf("name")))
            .assertFailure("noSuchMethod", "DataStream.map(", "DataStream.getTransformation()")
        assertEquals(4, result.refusals.size, result.refusals.toString())
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a call to the forwarder of a deprecated Flink method warns`(compiler: KotlinCompiler) {
        val result = ViewCodegen.compile(
            views(stream = listOf("writeUsingOutputFormat")),
            job(
                """
                import org.apache.flink.api.common.io.OutputFormat

                fun job(events: Stream<String>, format: OutputFormat<String>) = events.writeUsingOutputFormat(format)
                """,
            ),
            compiler = compiler,
        ).assertWarning("Deprecated in Flink: DataStream.writeUsingOutputFormat")
        assertEquals(listOf("DEPRECATION"), result.jobWarnings.map { it.name }, result.jobWarnings.toString())
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a call to the forwarder of an @Experimental Flink method warns without an opt-in`(compiler: KotlinCompiler) {
        val result = ViewCodegen.compile(
            views(stream = listOf("collectAsync")),
            job("fun job(events: Stream<String>) = events.collectAsync()"),
            compiler = compiler,
        ).assertWarning("This calls a Flink API that Apache Flink marks @Experimental")
        assertEquals(listOf("OPT_IN_USAGE"), result.jobWarnings.map { it.name }, result.jobWarnings.toString())
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a call to the forwarder of an @Experimental Flink method compiles cleanly with an opt-in`(compiler: KotlinCompiler) {
        val result = ViewCodegen.compile(
            views(stream = listOf("collectAsync")),
            job(
                """
                import io.flinkt.flink.ExperimentalFlinkApi

                @OptIn(ExperimentalFlinkApi::class)
                fun job(events: Stream<String>) = events.collectAsync()
                """,
            ),
            compiler = compiler,
        ).assertSuccess()
        assertEquals(emptyList<Any>(), result.jobWarnings)
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a method that only takes a deprecated type is forwarded without an annotation`(compiler: KotlinCompiler) {
        // addSink isn't deprecated, but the SinkFunction it takes is, on every line, in a package that differs.
        val sinkFunction = DataStream::class.java.getMethod("addSink", sinkFunctionClass()).parameterTypes.single().name
        val result = ViewCodegen.compile(
            views(stream = listOf("addSink")),
            job(
                """
                @Suppress("DEPRECATION")
                fun legacySink(): $sinkFunction<String> = TODO()

                fun job(events: Stream<String>) = events.addSink(legacySink())
                """,
            ),
            compiler = compiler,
        ).assertSuccess()
        assertEquals(emptyList<Any>(), result.jobWarnings)
        val addSink = result.generated("Stream").lines().indexOfFirst { "fun addSink(" in it }
        assertFalse("@Deprecated" in result.generated("Stream").lines()[addSink - 1])
    }

    @Test
    fun `a method of the superclass's Flink class is forwarded by the superclass's view`() {
        ViewCodegen.compile(views(operator = listOf("filter")))
            .assertFailure("Operator forwards filter, which DataStream declares. Stream views DataStream, and OperatorForwarders extends StreamForwarders: forward filter there.")
    }

    @Test
    fun `a forwarder of a method that a Flink subclass overrides overrides the superclass view's forwarder`() {
        // DataStreamSource overrides setParallelism, with its own class as the result. The names file has no
        // DataStreamSource, so the test adds its row.
        val flinkApi = editedFlinkApiFile(dir) { lines ->
            lines + (
                "org.apache.flink.streaming.api.datastream.DataStreamSource\tsetParallelism\tparallelism\t" +
                    "(I)Lorg/apache/flink/streaming/api/datastream/DataStreamSource;"
                )
        }
        val result = ViewCodegen.compile(
            views(operator = listOf("setParallelism")),
            SourceFile(
                "sample/Source.kt",
                """
                package sample

                import io.flinkt.codegen.FlinkView
                import org.apache.flink.streaming.api.datastream.DataStreamSource

                @FlinkView(DataStreamSource::class, forward = ["setParallelism"])
                class Source<T>(private val source: DataStreamSource<T>) : Operator<T>(source), SourceForwarders<T> {
                    override fun asFlink(): DataStreamSource<T> = source
                }
                """.trimIndent(),
            ),
            job(
                """
                import io.flinkt.testing.exactTypeOf
                import io.flinkt.testing.shouldBe

                fun job(source: Source<String>, operator: Operator<String>) {
                    exactTypeOf(source.setParallelism(2)).shouldBe<Source<String>>()
                    exactTypeOf((source as Operator<String>).setParallelism(2)).shouldBe<Operator<String>>()
                    exactTypeOf(operator.setParallelism(2)).shouldBe<Operator<String>>()
                }
                """,
            ),
            flinkApiFile = flinkApi,
        )
        result.assertSuccess()
        assertTrue("public override fun setParallelism(parallelism: kotlin.Int): sample.Source<T>" in result.generated("Source"))
    }

    @Test
    fun `a forwarded method without a row in the names file fails the build`() {
        val flinkApi = editedFlinkApiFile(dir) { lines -> lines.filterNot { "\tunion\t" in it } }
        ViewCodegen.compile(views(stream = listOf("union")), flinkApiFile = flinkApi)
            .assertFailure(
                "DataStream.union(DataStream<T>...) has no parameter names in the names file: it has no row for " +
                    "org.apache.flink.streaming.api.datastream.DataStream union " +
                    "([Lorg/apache/flink/streaming/api/datastream/DataStream;)Lorg/apache/flink/streaming/api/datastream/DataStream;",
            )
    }

    @Test
    fun `a names file extracted from another Flink version fails the build`() {
        val flinkApi = editedFlinkApiFile(dir) { lines -> lines.map { if (it.startsWith("flink-version\t")) "flink-version\t0.9.0" else it } }
        ViewCodegen.compile(views(stream = listOf("filter")), flinkApiFile = flinkApi)
            .assertFailure("was extracted from Flink 0.9.0, but the classpath has Flink $flinkVersion")
    }

    @Test
    fun `the generator checks the names file even without a view`() {
        val flinkApi = editedFlinkApiFile(dir) { lines -> lines.map { if (it.startsWith("flink-version\t")) "flink-version\t0.9.0" else it } }
        val result = ViewCodegen.compile(job("fun f() = 1"), flinkApiFile = flinkApi)
        result.assertFailure("was extracted from Flink 0.9.0")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
    }

    @Test
    fun `the names file records the Flink version on the adapter's classpath`() {
        assertTrue(ViewCodegen.flinkApiFile.readLines().contains("flink-version\t$flinkVersion"))
        // The Flink version on the classpath is the one this test runs on.
        assertTrue(flinkVersion.startsWith(ADAPTER_FLINK_LINE.toString() + "."), "$flinkVersion is on line $ADAPTER_FLINK_LINE")
    }

    private fun sinkFunctionClass(): Class<*> = DataStream::class.java.methods.single { it.name == "addSink" }.parameterTypes.single()
}
