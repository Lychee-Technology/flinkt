package io.flinkt.testing

import io.flinkt.testing.compile.CompilationResult
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class ExactTypeTest {

    @Test
    fun `an exact type compiles, a flexible type matching its non-null form`() {
        exactTypeOf(listOf("a")).shouldBe<List<String>>()
        exactTypeOf(System.getProperty("java.version")).shouldBe<String>()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `the same type compiles`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler).compile(snippet("exactTypeOf(Cat()).shouldBe<Cat>()")).assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a subtype doesn't compile`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(snippet("exactTypeOf(Cat()).shouldBe<Animal>()"))
            .assertSubtypeRejected()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `nullability is part of the type`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(
                snippet(
                    """
                    exactTypeOf(Cat()).shouldBe<Cat?>()
                    exactTypeOf<Cat?>(null).shouldBe<Cat>()
                    """,
                ),
            )
            .assertSubtypeRejected()
            .also { assertEquals(listOf(8, 9), it.errors.map { error -> error.line }) }
    }

    // The two compilers word this error differently. Its name, and this phrase, are what they share.
    private fun CompilationResult.assertSubtypeRejected(): CompilationResult =
        assertFailure("UNRESOLVED_REFERENCE_WRONG_RECEIVER", "receiver type mismatch")

    private fun snippet(body: String): String =
        """
        |import io.flinkt.testing.exactTypeOf
        |import io.flinkt.testing.shouldBe
        |
        |open class Animal
        |class Cat : Animal()
        |
        |fun check() {
        |${body.trimIndent().prependIndent("    ")}
        |}
        """.trimMargin()
}
