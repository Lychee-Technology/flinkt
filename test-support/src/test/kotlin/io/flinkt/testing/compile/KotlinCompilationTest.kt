package io.flinkt.testing.compile

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class KotlinCompilationTest {
    @Test
    fun `the compilers are the pinned build compiler and the minimum, 2_4_0`() {
        assertEquals("2.4.0", KotlinCompiler.MINIMUM.version)
        val stable = Regex("""(\d+)\.(\d+)\.(\d+)""")
        val (major, minor) = stable.matchEntire(KotlinCompiler.BUILD.version)
            ?.destructured?.let { (a, b) -> a.toInt() to b.toInt() }
            ?: error("the build compiler ${KotlinCompiler.BUILD.version} isn't a stable release")
        assertTrue(major > 2 || major == 2 && minor >= 4, "the build compiler is older than 2.4.0")
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `compiled classes load and run`(compiler: KotlinCompiler) {
        val result = KotlinCompilation(compiler).compile("fun answer(): Int = 6 * 7").assertSuccess()
        val answer = result.classLoader().loadClass("SnippetKt").getMethod("answer").invoke(null)
        assertEquals(42, answer)
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a failing compile reports each error with its location and name`(compiler: KotlinCompiler) {
        val result = KotlinCompilation(compiler)
            .compile(
                """
                fun f(): Int {
                    return missing()
                }
                """,
            )
            .assertFailure("Unresolved reference 'missing'")
        val error = result.errors.single()
        assertEquals(Origin.COMPILER, error.origin)
        assertEquals("Snippet.kt", error.file)
        assertEquals(2, error.line)
        assertEquals(12, error.column)
        assertEquals("UNRESOLVED_REFERENCE", error.name)
    }

    @Test
    fun `warnings are reported on a successful compile`() {
        KotlinCompilation()
            .compile(
                """
                @Deprecated("Use g")
                fun f() {}

                fun main() = f()
                """,
            )
            .assertWarning("DEPRECATION")
            .assertWarning("Use g")
    }

    @Test
    fun `the assertions reject the outcome they don't expect`() {
        val success = KotlinCompilation().compile("val x = 1")
        assertThrows<AssertionError> { success.assertFailure("anything") }
        val failure = KotlinCompilation().compile("val x: String = 1")
        assertThrows<AssertionError> { failure.assertSuccess() }
        assertThrows<AssertionError> { failure.assertFailure("a message the compiler doesn't print") }
        assertThrows<AssertionError> { failure.assertWarning("anything") }
    }
}
