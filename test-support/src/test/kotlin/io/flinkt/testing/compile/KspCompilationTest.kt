package io.flinkt.testing.compile

import java.io.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class KspCompilationTest {
    private val processor: List<File> =
        System.getProperty("flinkt.test.processorClasspath").split(File.pathSeparatorChar).map(::File)

    private val greet = SourceFile("sample/Greet.kt", "package sample\n\nannotation class Greet\n")

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `sources compile together with the code KSP generated from them`(compiler: KotlinCompiler) {
        val result = KotlinCompilation(compiler, kspProcessorClasspath = processor)
            .compile(
                greet,
                SourceFile("sample/User.kt", "package sample\n\n@Greet\nclass User\n\nfun greeting(): String = UserGreeting.TEXT\n"),
            )
            .assertSuccess()
        val greeting = result.classLoader().loadClass("sample.UserKt").getMethod("greeting").invoke(null)
        assertEquals("Hello, User", greeting)
    }

    @Test
    fun `a processor error fails the compilation, located at the declaration`() {
        val result = KotlinCompilation(kspProcessorClasspath = processor)
            .compile(greet, SourceFile("sample/Bad.kt", "package sample\n\n@Greet\nobject Bad\n"))
            .assertFailure("@Greet applies to classes only")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
        val error = result.errors.single()
        assertEquals(Origin.KSP, error.origin)
        assertEquals("sample/Bad.kt", error.file)
        assertEquals(4, error.line) // `object Bad`: KSP locates a declaration at its name, not its annotation

    }
}
