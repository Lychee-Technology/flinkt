package io.flinkt.flink

import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
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
