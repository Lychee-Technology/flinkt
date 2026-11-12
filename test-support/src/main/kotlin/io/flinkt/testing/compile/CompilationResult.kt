package io.flinkt.testing.compile

import java.io.File
import java.net.URLClassLoader

/** What a [KotlinCompilation] produced. The `assert` functions throw an [AssertionError] that includes the output. */
public class CompilationResult internal constructor(
    public val compiler: KotlinCompiler,
    /** The compiler's exit code, such as `OK` or `COMPILATION_ERROR`, or [KSP_ERROR] if KSP failed first. */
    public val exitCode: String,
    public val diagnostics: List<Diagnostic>,
    /** Everything KSP and the compiler printed. */
    public val output: String,
    /** The directory holding this compilation's sources, generated code and classes. */
    public val workDir: File,
    public val classesDir: File,
    public val classpath: List<File>,
) {
    public val succeeded: Boolean get() = exitCode == "OK"

    public val errors: List<Diagnostic> get() = diagnostics.filter { it.severity == Severity.ERROR }

    public val warnings: List<Diagnostic> get() = diagnostics.filter { it.severity == Severity.WARNING }

    public fun assertSuccess(): CompilationResult {
        if (!succeeded) fail("Expected the compilation to succeed, but it ended with $exitCode")
        return this
    }

    /**
     * Asserts that the compilation failed with errors, not a crash, and that each expected text appears in an error,
     * as rendered by [Diagnostic.toString]: a message, or a diagnostic name such as `UNRESOLVED_REFERENCE`.
     *
     * Compiler versions word some messages differently, so a test that runs on both [KotlinCompiler]s matches the
     * diagnostic name and the text they share.
     */
    public fun assertFailure(expectedError: String, vararg moreExpectedErrors: String): CompilationResult {
        if (succeeded) fail("Expected the compilation to fail, but it succeeded")
        if (exitCode != "COMPILATION_ERROR" && exitCode != KSP_ERROR) fail("Expected compilation errors, but it ended with $exitCode")
        val missing = (listOf(expectedError) + moreExpectedErrors).filter { expected -> errors.none { expected in it.toString() } }
        if (missing.isNotEmpty()) fail("Expected errors containing ${missing.joinToString { "\"$it\"" }}")
        return this
    }

    /** Asserts that the compilation succeeded and a warning contains [expectedWarning]. */
    public fun assertWarning(expectedWarning: String): CompilationResult {
        assertSuccess()
        if (warnings.none { expectedWarning in it.toString() }) fail("Expected a warning containing \"$expectedWarning\"")
        return this
    }

    /**
     * A class loader for the compiled classes. [parent] has to supply what they were compiled against, which the
     * test's own class loader does for the default classpath.
     */
    public fun classLoader(parent: ClassLoader = CompilationResult::class.java.classLoader): ClassLoader {
        assertSuccess()
        return URLClassLoader(arrayOf(classesDir.toURI().toURL()), parent)
    }

    private fun fail(message: String): Nothing =
        throw AssertionError(
            "$message (Kotlin ${compiler.version}, in $workDir).\n" +
                "Diagnostics:\n" + diagnostics.joinToString("\n") { "  $it" }.ifEmpty { "  none" } +
                "\nOutput:\n" + output.ifBlank { "(none)" },
        )

    public companion object {
        /** The [exitCode] of a compilation whose KSP run failed, so the compiler never ran. */
        public const val KSP_ERROR: String = "KSP_ERROR"
    }
}
