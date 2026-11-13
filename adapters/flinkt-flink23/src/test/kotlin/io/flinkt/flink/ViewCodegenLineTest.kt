package io.flinkt.flink

import io.flinkt.flink.ViewCodegen.job
import io.flinkt.flink.ViewCodegen.jobWarnings
import io.flinkt.flink.ViewCodegen.views
import io.flinkt.testing.compile.KotlinCompiler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/** The generator on what only Flink 2.3 has, or lacks. The shared `ViewCodegenTest` covers the rest. */
class ViewCodegenLineTest {
    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `2_x's @Experimental enableAsyncState is forwarded behind the opt-in`(compiler: KotlinCompiler) {
        val views = views(operator = listOf("enableAsyncState"), keyed = listOf("enableAsyncState"))
        val warned = ViewCodegen.compile(
            views,
            job(
                """
                import io.flinkt.testing.exactTypeOf
                import io.flinkt.testing.shouldBe

                fun job(operator: Operator<String>, keyed: Keyed<String, Int>) {
                    exactTypeOf(operator.enableAsyncState()).shouldBe<Operator<String>>()
                    exactTypeOf(keyed.enableAsyncState()).shouldBe<Keyed<String, Int>>()
                }
                """,
            ),
            compiler = compiler,
        ).assertWarning("This calls a Flink API that Apache Flink marks @Experimental")
        assertEquals(listOf("OPT_IN_USAGE", "OPT_IN_USAGE"), warned.jobWarnings.map { it.name }, warned.jobWarnings.toString())

        val optedIn = ViewCodegen.compile(
            views,
            job(
                """
                import io.flinkt.flink.ExperimentalFlinkApi

                @OptIn(ExperimentalFlinkApi::class)
                fun job(operator: Operator<String>, keyed: Keyed<String, Int>) {
                    operator.enableAsyncState()
                    keyed.enableAsyncState()
                }
                """,
            ),
            compiler = compiler,
        ).assertSuccess()
        assertEquals(emptyList<Any>(), optedIn.jobWarnings)
    }

    @Test
    fun `a method that only 1_20 has fails the build on 2_3`() {
        ViewCodegen.compile(views(stream = listOf("iterate")))
            .assertFailure("Stream forwards iterate, but DataStream has no public method iterate in Flink 2.3.0")
    }
}
