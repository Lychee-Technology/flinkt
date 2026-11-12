package io.flinkt.testing.compile

import java.io.File

/** The system properties that the `flinkt.compile-tests` Gradle convention passes to a test JVM. */
internal object HarnessProperties {
    fun string(name: String): String =
        System.getProperty(name)
            ?: throw IllegalStateException(
                "System property '$name' isn't set. The compile-test harness gets its compilers and classpaths " +
                    "from Gradle: run the test through Gradle, in a module that applies flinkt.compile-tests.",
            )

    fun files(name: String): List<File> = string(name).split(File.pathSeparatorChar).filter { it.isNotEmpty() }.map(::File)

    val moduleClasspath: List<File> by lazy { files("flinkt.compileTest.module.classpath") }

    val kspVersion: String by lazy { string("flinkt.compileTest.ksp.version") }

    val kspClasspath: List<File> by lazy { files("flinkt.compileTest.ksp.classpath") }

    val workDir: File by lazy { File(string("flinkt.compileTest.workDir")) }
}
