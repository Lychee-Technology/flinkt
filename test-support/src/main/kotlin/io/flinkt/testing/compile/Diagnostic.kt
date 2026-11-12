package io.flinkt.testing.compile

import java.io.File

public enum class Severity { ERROR, WARNING, INFO }

public enum class Origin { COMPILER, KSP }

/** One message of KSP or the compiler. [file] is relative to the source root it was found in. */
public data class Diagnostic(
    val origin: Origin,
    val severity: Severity,
    val file: String?,
    val line: Int?,
    val column: Int?,
    /** The compiler's name for the diagnostic, such as `UNRESOLVED_REFERENCE`. KSP messages have none. */
    val name: String?,
    val message: String,
) {
    override fun toString(): String = buildString {
        if (file != null) append(file).append(':').append(line).append(':').append(column).append(": ")
        append(severity.name.lowercase()).append(": ")
        if (origin == Origin.KSP) append("[ksp] ")
        if (name != null) append('[').append(name).append("] ")
        append(message)
    }
}

internal object Diagnostics {
    // `/abs/Snippet.kt:3:5: error: [NAME] message`, or without a location. Lines that follow, up to the next
    // header, belong to the message: the rest of a multi-line message, and the source excerpt.
    private val compilerHeader =
        Regex("""^(?:(.+?):(\d+):(\d+): )?(error|warning|strong warning|info|exception): (?:\[([A-Z0-9_]+)] )?(.*)$""")

    // `e: [ksp] /abs/Snippet.kt:3: message`, `w: [ksp] message`
    private val kspHeader = Regex("""^([ewiv]): \[ksp] (?:(/.+?|[A-Za-z]:\\.+?):(\d+): )?(.*)$""")

    fun parseCompiler(output: String, sourceRoots: List<File>): List<Diagnostic> {
        val diagnostics = mutableListOf<Diagnostic>()
        for (line in output.lines()) {
            val match = compilerHeader.matchEntire(line)
            if (match == null) {
                if (diagnostics.isNotEmpty() && line.isNotBlank()) {
                    val last = diagnostics.removeAt(diagnostics.lastIndex)
                    diagnostics += last.copy(message = last.message + "\n" + line)
                }
                continue
            }
            val (path, lineNumber, column, severity, name, message) = match.destructured
            diagnostics += Diagnostic(
                origin = Origin.COMPILER,
                severity = when (severity) {
                    "error", "exception" -> Severity.ERROR
                    "warning", "strong warning" -> Severity.WARNING
                    else -> Severity.INFO
                },
                file = path.ifEmpty { null }?.let { relativize(it, sourceRoots) },
                line = lineNumber.toIntOrNull(),
                column = column.toIntOrNull(),
                name = name.ifEmpty { null },
                message = message,
            )
        }
        return diagnostics
    }

    fun parseKsp(output: String, sourceRoot: File): List<Diagnostic> = output.lines().mapNotNull { line ->
        val match = kspHeader.matchEntire(line) ?: return@mapNotNull null
        val (level, path, lineNumber, message) = match.destructured
        Diagnostic(
            origin = Origin.KSP,
            severity = when (level) {
                "e" -> Severity.ERROR
                "w" -> Severity.WARNING
                else -> Severity.INFO
            },
            file = path.ifEmpty { null }?.let { relativize(it, listOf(sourceRoot)) },
            line = lineNumber.toIntOrNull(),
            column = null,
            name = null,
            message = message,
        )
    }

    private fun relativize(path: String, roots: List<File>): String {
        val file = File(path).absoluteFile
        val root = roots.firstOrNull { file.startsWith(it.absoluteFile) } ?: return path
        return file.relativeTo(root.absoluteFile).invariantSeparatorsPath
    }
}
