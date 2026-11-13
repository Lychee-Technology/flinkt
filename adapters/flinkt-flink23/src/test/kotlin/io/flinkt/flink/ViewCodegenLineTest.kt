package io.flinkt.flink

import io.flinkt.flink.ViewCodegen.job
import io.flinkt.flink.ViewCodegen.jobWarnings
import io.flinkt.flink.ViewCodegen.refusals
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
    fun `2_x's KeyedStream declares one flatMap and inherits the other, and the build checks each`() {
        // KeyedStream overrides flatMap(FlatMapFunction, TypeInformation) and inherits DataStream's flatMap(FlatMapFunction).
        val unlisted = ViewCodegen.compile(views(keyed = listOf("flatMap")))
            .assertFailure(
                "Keyed forwards flatMap, which covers all of its overloads, but KeyedStream inherits " +
                    "DataStream.flatMap(FlatMapFunction<T, R>), and Stream, the view of DataStream, doesn't forward flatMap. " +
                    "List flatMap on Stream too",
                "KeyedStream.flatMap(FlatMapFunction<T, R>, TypeInformation<R>) returns SingleOutputStreamOperator<R>, a type built from its type parameter",
            )
        assertEquals(2, unlisted.refusals.size, unlisted.refusals.toString())

        // Listed on Stream too, DataStream's overloads are Stream's, and each is refused there.
        val listed = ViewCodegen.compile(views(stream = listOf("flatMap"), keyed = listOf("flatMap")))
            .assertFailure(
                "DataStream.flatMap(FlatMapFunction<T, R>) returns SingleOutputStreamOperator<R>",
                "DataStream.flatMap(FlatMapFunction<T, R>, TypeInformation<R>) returns SingleOutputStreamOperator<R>",
                "KeyedStream.flatMap(FlatMapFunction<T, R>, TypeInformation<R>) returns SingleOutputStreamOperator<R>",
            )
        assertEquals(3, listed.refusals.size, listed.refusals.toString())
    }

    @Test
    fun `a method that only 1_20 has fails the build on 2_3`() {
        ViewCodegen.compile(views(stream = listOf("iterate")))
            .assertFailure("Stream forwards iterate, but DataStream has no public method iterate in Flink 2.3.0")
    }
}
