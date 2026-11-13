package io.flinkt.flink

import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * The compile-test harness against this adapter's classpath: the adapter, flinkt-core through it, and the line's
 * Flink, as a user's job compiles against them.
 */
class AdapterCompileTest {
    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a job's exact stream type compiles`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler).compile(job("exactTypeOf(users).shouldBe<SingleOutputStreamOperator<User>>()")).assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a supertype of the stream doesn't compile as its exact type`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(job("exactTypeOf(users).shouldBe<DataStream<User>>()"))
            .assertFailure("UNRESOLVED_REFERENCE_WRONG_RECEIVER", "receiver type mismatch")
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `typeInfo inlined by a user's compiler gives Flink's TypeInformation`(compiler: KotlinCompiler) {
        val result = KotlinCompilation(compiler)
            .compile(
                """
                import io.flinkt.flink.typeInfo
                import io.flinkt.testing.exactTypeOf
                import io.flinkt.testing.shouldBe
                import org.apache.flink.api.common.typeinfo.TypeInformation

                fun ids(): TypeInformation<Long> {
                    val ids = typeInfo<Long>()
                    exactTypeOf(ids).shouldBe<TypeInformation<Long>>()
                    return ids
                }
                """,
            )
            .assertSuccess()
        val ids = result.classLoader().loadClass("SnippetKt").getMethod("ids").invoke(null)
        assertSame(BasicTypeInfo.LONG_TYPE_INFO, ids)
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `typeInfo of a type that isn't known at the call site doesn't compile`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(
                """
                import io.flinkt.flink.typeInfo

                fun <T> elements() = typeInfo<T>()
                """,
            )
            .assertFailure("TYPE_PARAMETER_AS_REIFIED", "Cannot use 'T' as reified type parameter")
    }

    private fun job(assertion: String): String =
        """
        |import io.flinkt.FlinkType
        |import io.flinkt.testing.exactTypeOf
        |import io.flinkt.testing.shouldBe
        |import org.apache.flink.api.common.functions.MapFunction
        |import org.apache.flink.streaming.api.datastream.DataStream
        |import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
        |
        |@FlinkType
        |data class User(val id: Long, val name: String)
        |
        |fun users(names: DataStream<String>) {
        |    val users = names.map(MapFunction<String, User> { User(0, it) })
        |    $assertion
        |}
        """.trimMargin()
}
