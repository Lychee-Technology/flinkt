package io.flinkt.testing.compile

import java.io.File
import java.util.concurrent.TimeUnit

internal class KspRun(val exitCode: Int, val output: String, val kotlinDir: File, val resourcesDir: File)

/**
 * Runs the pinned KSP2 command-line processor in a child JVM. Its entry point ends the JVM it runs in, so it can't
 * run inside the test JVM.
 */
internal object KspRunner {
    private const val MAIN = "com.google.devtools.ksp.cmdline.KSPJvmMain"
    private const val TIMEOUT_MINUTES = 5L

    fun run(
        compiler: KotlinCompiler,
        sourceRoot: File,
        classpath: List<File>,
        processors: List<File>,
        options: Map<String, String>,
        outputDir: File,
    ): KspRun {
        val kotlinDir = File(outputDir, "kotlin")
        val resourcesDir = File(outputDir, "resources")
        val separator = File.pathSeparator
        val command = listOf(
            File(System.getProperty("java.home"), if (File.separatorChar == '\\') "bin/java.exe" else "bin/java").path,
            "-cp", HarnessProperties.kspClasspath.joinToString(separator),
            MAIN,
            "-module-name=snippet",
            "-jvm-target=1.8",
            // KSP analyzes with its own bundled Kotlin. It's told the language version the compiler will use.
            "-language-version=${compiler.languageVersion}",
            "-api-version=${compiler.languageVersion}",
            "-jdk-home=${System.getProperty("java.home")}",
            "-source-roots=${sourceRoot.path}",
            "-libraries=${classpath.joinToString(separator)}",
            "-project-base-dir=${sourceRoot.parentFile.path}",
            "-output-base-dir=${outputDir.path}",
            "-caches-dir=${File(outputDir, "caches").path}",
            "-class-output-dir=${File(outputDir, "classes").path}",
            "-kotlin-output-dir=${kotlinDir.path}",
            "-java-output-dir=${File(outputDir, "java").path}",
            "-resource-output-dir=${resourcesDir.path}",
        ) + options.takeIf { it.isNotEmpty() }
            ?.let { listOf("-processor-options=" + it.entries.joinToString(separator) { (k, v) -> "$k=$v" }) }
            .orEmpty() +
            processors.joinToString(separator)

        outputDir.mkdirs()
        val logFile = File(outputDir, "ksp.log")
        val process = ProcessBuilder(command).redirectErrorStream(true).redirectOutput(logFile).start()
        if (!process.waitFor(TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
            process.destroyForcibly()
            throw AssertionError("KSP didn't finish within $TIMEOUT_MINUTES minutes; its output is in $logFile")
        }
        return KspRun(process.exitValue(), logFile.readText(), kotlinDir, resourcesDir)
    }
}
