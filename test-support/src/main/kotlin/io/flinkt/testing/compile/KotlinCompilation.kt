package io.flinkt.testing.compile

import java.io.File
import java.nio.file.Files

/** A source file of a compilation, at [path] relative to the source root. */
public class SourceFile(public val path: String, public val content: String)

/**
 * Compiles Kotlin snippets the way a user's build would compile them against a Flinkt module.
 *
 * ```kotlin
 * KotlinCompilation(KotlinCompiler.MINIMUM)
 *     .compile("""fun f(x: Int): String = x""")
 *     .assertFailure("Return type mismatch")
 * ```
 *
 * The default [classpath] is the module under test as a consumer compiles against it: its API and the API of its
 * dependencies, an adapter's Flink line, and `:test-support` for `exactTypeOf`. With a [kspProcessorClasspath], KSP
 * runs first with those processors, and the compiler then compiles the sources together with the Kotlin that KSP
 * generated. Java that KSP generates isn't compiled.
 *
 * Each compilation keeps its sources, generated code and classes in a directory under the module's
 * `build/compile-tests`, which a failure message names.
 */
public class KotlinCompilation(
    public val compiler: KotlinCompiler = KotlinCompiler.BUILD,
    public val classpath: List<File> = HarnessProperties.moduleClasspath,
    public val kspProcessorClasspath: List<File> = emptyList(),
    public val kspProcessorOptions: Map<String, String> = emptyMap(),
    public val compilerArguments: List<String> = emptyList(),
) {
    /** Compiles one file, `Snippet.kt`, holding [code] with its common indent removed. */
    public fun compile(code: String): CompilationResult = compile(SourceFile("Snippet.kt", code.trimIndent()))

    public fun compile(vararg sources: SourceFile): CompilationResult {
        require(sources.isNotEmpty()) { "Nothing to compile" }
        val workDir = HarnessProperties.workDir.also { it.mkdirs() }
        val root = Files.createTempDirectory(workDir.toPath(), "${compiler.name.lowercase()}-").toFile()
        val sourceRoot = File(root, "src")
        for (source in sources) {
            File(sourceRoot, source.path).apply { parentFile.mkdirs() }.writeText(source.content)
        }
        val classesDir = File(root, "classes").apply { mkdirs() }

        val ksp = if (kspProcessorClasspath.isEmpty()) {
            null
        } else {
            KspRunner.run(compiler, sourceRoot, classpath, kspProcessorClasspath, kspProcessorOptions, File(root, "ksp"))
        }
        val kspDiagnostics = ksp?.let { Diagnostics.parseKsp(it.output, sourceRoot) }.orEmpty()
        if (ksp != null && (ksp.exitCode != 0 || kspDiagnostics.any { it.severity == Severity.ERROR })) {
            return CompilationResult(compiler, CompilationResult.KSP_ERROR, kspDiagnostics, ksp.output, root, classesDir, classpath)
        }

        val sourceRoots = listOfNotNull(sourceRoot, ksp?.kotlinDir?.takeIf { it.exists() })
        val arguments = listOf(
            "-d", classesDir.path,
            "-classpath", classpath.joinToString(File.pathSeparator),
            "-no-stdlib",
            "-no-reflect",
            "-module-name", "snippet",
            "-Xrender-internal-diagnostic-names",
        ) + compilerArguments + sourceRoots.map { it.path }
        val run = compiler.run(arguments)
        if (run.exitCode == "OK") ksp?.resourcesDir?.takeIf { it.exists() }?.copyRecursively(classesDir, overwrite = true)

        val output = listOfNotNull(ksp?.output?.takeIf { it.isNotBlank() }, run.output).joinToString("\n")
        val diagnostics = kspDiagnostics + Diagnostics.parseCompiler(run.output, sourceRoots)
        return CompilationResult(compiler, run.exitCode, diagnostics, output, root, classesDir, classpath)
    }
}
