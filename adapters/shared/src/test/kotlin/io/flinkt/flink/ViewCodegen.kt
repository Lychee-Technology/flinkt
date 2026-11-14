package io.flinkt.flink

import io.flinkt.testing.compile.CompilationResult
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.SourceFile
import java.io.File

/**
 * Runs `flinkt-view-codegen` in the compile-test harness, as the adapter's build runs it: with this adapter's names
 * file, the Flink version on its classpath, and its Flink line's classes.
 *
 * [views] declares three views in package `sample`, related as the adapter's are: `Stream<T>` over `DataStream`, and
 * `Operator<T>` over `SingleOutputStreamOperator` and `Keyed<T, K>` over `KeyedStream`, which extend it. Each lists
 * the methods a test forwards. `filter` returns a `SingleOutputStreamOperator`, and `reduce` too, so a test that
 * forwards a method returning a stream needs all three views anyway.
 */
object ViewCodegen {
    val processor: List<File> = property("classpath").split(File.pathSeparatorChar).map(::File)

    /** The adapter's checked-in `flink-api.tsv`. */
    val flinkApiFile: File = File(property("flinkApiFile"))

    /** The version of the Flink on the adapter's compile classpath, such as `2.3.0`. */
    val flinkVersion: String = property("flinkVersion")

    /** The Kotlin that the generator wrote in each adapter's build, this one's included: one directory per adapter. */
    val adapterForwarders: List<File> = property("adapterForwarders").split(File.pathSeparatorChar).map(::File)

    fun compile(
        vararg sources: SourceFile,
        compiler: KotlinCompiler = KotlinCompiler.BUILD,
        flinkApiFile: File = this.flinkApiFile,
        flinkVersion: String = this.flinkVersion,
    ): CompilationResult = KotlinCompilation(
        compiler,
        kspProcessorClasspath = processor,
        kspProcessorOptions = mapOf("flinkt.view.flinkApiFile" to flinkApiFile.path, "flinkt.view.flinkVersion" to flinkVersion),
    ).compile(*sources)

    fun views(
        stream: List<String> = emptyList(),
        operator: List<String> = emptyList(),
        keyed: List<String> = emptyList(),
    ): SourceFile = SourceFile(
        "sample/Views.kt",
        """
        |package sample
        |
        |import io.flinkt.codegen.FlinkView
        |import org.apache.flink.streaming.api.datastream.DataStream
        |import org.apache.flink.streaming.api.datastream.KeyedStream
        |import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
        |
        |@FlinkView(DataStream::class, forward = [${stream.joinToString { "\"$it\"" }}])
        |open class Stream<T>(private val stream: DataStream<T>) : StreamForwarders<T> {
        |    override fun asFlink(): DataStream<T> = stream
        |}
        |
        |@FlinkView(SingleOutputStreamOperator::class, forward = [${operator.joinToString { "\"$it\"" }}])
        |open class Operator<T>(private val operator: SingleOutputStreamOperator<T>) : Stream<T>(operator), OperatorForwarders<T> {
        |    override fun asFlink(): SingleOutputStreamOperator<T> = operator
        |}
        |
        |@FlinkView(KeyedStream::class, forward = [${keyed.joinToString { "\"$it\"" }}])
        |class Keyed<T, K>(private val keyed: KeyedStream<T, K>) : Stream<T>(keyed), KeyedForwarders<T, K> {
        |    override fun asFlink(): KeyedStream<T, K> = keyed
        |}
        """.trimMargin(),
    )

    /** A source file `sample/Job.kt` holding [code] in package `sample`. */
    fun job(code: String): SourceFile = SourceFile("sample/Job.kt", "package sample\n\n" + code.trimIndent() + "\n")

    /** The Kotlin that KSP generated for [view], such as `StreamForwarders`. */
    fun CompilationResult.generated(view: String): String = File(workDir, "ksp/kotlin/sample/${view}Forwarders.kt").readText()

    /**
     * The generator's errors, which it locates at the view declarations. After a refusal, KSP also reports the views'
     * generated supertypes as unresolved, because it stops before it compiles what it generated.
     */
    val CompilationResult.refusals get() = errors.filter { it.file == "sample/Views.kt" }

    /** The compiler's warnings located in the job, not in the views or the generated code. */
    val CompilationResult.jobWarnings get() = warnings.filter { it.file == "sample/Job.kt" }

    /** A copy of the adapter's names file, edited by [edit], in [dir]. */
    fun editedFlinkApiFile(dir: File, edit: (List<String>) -> List<String>): File =
        File(dir, "flink-api.tsv").apply { writeText(edit(flinkApiFile.readLines()).joinToString("\n", postfix = "\n")) }

    private fun property(name: String): String =
        System.getProperty("flinkt.test.viewCodegen.$name")
            ?: throw IllegalStateException("flinkt.test.viewCodegen.$name isn't set: run the test through the adapter's Gradle build")
}
