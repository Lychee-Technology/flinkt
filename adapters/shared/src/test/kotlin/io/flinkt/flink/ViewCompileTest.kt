package io.flinkt.flink

import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.SourceFile
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * The views' compile contracts (docs/testing.md, "Kotlin API compile contracts"), compiled by both compilers against
 * the adapter's own Flink line, as a user's job compiles: entering and leaving, each form an operator can be called
 * in with the exact static type it gives, and what mustn't compile.
 */
class ViewCompileTest {
    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `each Flink stream enters as its view, and asFlink has its exact Flink type`(compiler: KotlinCompiler) {
        compile(
            compiler,
            """
            fun entry(
                stream: DataStream<Event>,
                operator: SingleOutputStreamOperator<Event>,
                source: DataStreamSource<Event>,
                sideOutput: SideOutputDataStream<Event>,
                keyed: KeyedStream<Event, Long>,
            ) {
                exactTypeOf(stream.flinkt()).shouldBe<FlinktDataStream<Event>>()
                exactTypeOf(stream.flinkt().asFlink()).shouldBe<DataStream<Event>>()
                exactTypeOf(operator.flinkt()).shouldBe<FlinktSingleOutputStreamOperator<Event>>()
                exactTypeOf(operator.flinkt().asFlink()).shouldBe<SingleOutputStreamOperator<Event>>()
                exactTypeOf(source.flinkt()).shouldBe<FlinktSingleOutputStreamOperator<Event>>()
                exactTypeOf(source.flinkt().asFlink()).shouldBe<SingleOutputStreamOperator<Event>>()
                exactTypeOf(sideOutput.flinkt()).shouldBe<FlinktDataStream<Event>>()
                exactTypeOf(keyed.flinkt()).shouldBe<FlinktKeyedStream<Event, Long>>()
                exactTypeOf(keyed.flinkt().asFlink()).shouldBe<KeyedStream<Event, Long>>()
            }
            """,
        ).assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `map takes its result type from the call site in every form`(compiler: KotlinCompiler) {
        compile(
            compiler,
            """
            fun map(events: FlinktDataStream<Event>, userType: TypeInformation<User>) {
                // lambda
                exactTypeOf(events.map { User(it.id, it.name) }).shouldBe<FlinktSingleOutputStreamOperator<User>>()
                // Flink function literal
                exactTypeOf(events.map(MapFunction<Event, User> { User(it.id, it.name) })).shouldBe<FlinktSingleOutputStreamOperator<User>>()
                // Kotlin function class
                exactTypeOf(events.map(ToUser())).shouldBe<FlinktSingleOutputStreamOperator<User>>()
                // Java function class
                exactTypeOf(events.map(JavaToUser())).shouldBe<FlinktSingleOutputStreamOperator<User>>()
                // generic function class
                exactTypeOf(events.map(Identity())).shouldBe<FlinktSingleOutputStreamOperator<Event>>()
                // rich function
                exactTypeOf(events.map(RichToUser())).shouldBe<FlinktSingleOutputStreamOperator<User>>()
                // Kotlin nullable result
                exactTypeOf(events.map { it.takeIf { e -> e.name.isNotEmpty() } }).shouldBe<FlinktSingleOutputStreamOperator<Event?>>()
                // explicit type argument
                exactTypeOf(events.map<User?>(JavaToUser())).shouldBe<FlinktSingleOutputStreamOperator<User?>>()
                // explicit TypeInformation
                exactTypeOf(events.map(ToUser(), userType)).shouldBe<FlinktSingleOutputStreamOperator<User>>()
                exactTypeOf(events.map(ToUser()).asFlink()).shouldBe<SingleOutputStreamOperator<User>>()
            }
            """,
        ).assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `keyBy takes its key type from the call site in every form`(compiler: KotlinCompiler) {
        compile(
            compiler,
            """
            fun keyBy(users: FlinktDataStream<User>, names: FlinktSingleOutputStreamOperator<String>, idType: TypeInformation<Long>) {
                // lambda
                exactTypeOf(users.keyBy { it.id }).shouldBe<FlinktKeyedStream<User, Long>>()
                // Flink function literal
                exactTypeOf(users.keyBy(KeySelector<User, Long> { it.id })).shouldBe<FlinktKeyedStream<User, Long>>()
                // Kotlin function class
                exactTypeOf(users.keyBy(ById())).shouldBe<FlinktKeyedStream<User, Long>>()
                // Java function class
                exactTypeOf(users.keyBy(JavaById())).shouldBe<FlinktKeyedStream<User, Long>>()
                // generic function class
                exactTypeOf(names.keyBy(Self())).shouldBe<FlinktKeyedStream<String, String>>()
                // Kotlin nullable result
                exactTypeOf(users.keyBy { it.name.takeIf { n -> n.isNotEmpty() } }).shouldBe<FlinktKeyedStream<User, String?>>()
                // explicit type argument
                exactTypeOf(users.keyBy<Long?>(JavaById())).shouldBe<FlinktKeyedStream<User, Long?>>()
                // explicit TypeInformation
                exactTypeOf(users.keyBy(ById(), idType)).shouldBe<FlinktKeyedStream<User, Long>>()
                exactTypeOf(users.keyBy(ById()).asFlink()).shouldBe<KeyedStream<User, Long>>()
            }
            """,
        ).assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `keyed process takes its result type from the call site in every form`(compiler: KotlinCompiler) {
        // KeyedProcessFunction is an abstract class, so it has no lambda form. An object expression is its literal.
        compile(
            compiler,
            """
            fun process(keyed: FlinktKeyedStream<User, Long>, sessionType: TypeInformation<Session>) {
                // Flink function literal
                val literal = keyed.process(
                    object : KeyedProcessFunction<Long, User, Session>() {
                        override fun processElement(value: User, ctx: Context, out: Collector<Session>) = Unit
                    },
                )
                exactTypeOf(literal).shouldBe<FlinktSingleOutputStreamOperator<Session>>()
                // Kotlin function class
                exactTypeOf(keyed.process(SessionFunction())).shouldBe<FlinktSingleOutputStreamOperator<Session>>()
                // Java function class
                exactTypeOf(keyed.process(JavaSessionFunction())).shouldBe<FlinktSingleOutputStreamOperator<Session>>()
                // generic function class
                exactTypeOf(keyed.process(Passthrough())).shouldBe<FlinktSingleOutputStreamOperator<User>>()
                // Kotlin nullable result
                val nullable = keyed.process(
                    object : KeyedProcessFunction<Long, User, Session?>() {
                        override fun processElement(value: User, ctx: Context, out: Collector<Session?>) = Unit
                    },
                )
                exactTypeOf(nullable).shouldBe<FlinktSingleOutputStreamOperator<Session?>>()
                // explicit type argument
                exactTypeOf(keyed.process<Session?>(JavaSessionFunction())).shouldBe<FlinktSingleOutputStreamOperator<Session?>>()
                // explicit TypeInformation
                exactTypeOf(keyed.process(SessionFunction(), sessionType)).shouldBe<FlinktSingleOutputStreamOperator<Session>>()
            }
            """,
        ).assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a view isn't a Flink stream`(compiler: KotlinCompiler) {
        compile(compiler, "fun leave(view: FlinktDataStream<Event>) { val d: DataStream<Event> = view }")
            .assertFailure("INITIALIZER_TYPE_MISMATCH")
        compile(compiler, "fun keyed(view: FlinktKeyedStream<Event, Long>) { val d: KeyedStream<Event, Long> = view }")
            .assertFailure("INITIALIZER_TYPE_MISMATCH")
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a Flink method the view doesn't offer doesn't compile on it`(compiler: KotlinCompiler) {
        compile(compiler, "fun connect(view: FlinktDataStream<Event>, other: DataStream<User>) = view.connect(other)")
            .assertFailure("UNRESOLVED_REFERENCE", "Unresolved reference 'connect'")
        // It does on asFlink().
        compile(compiler, "fun connect(view: FlinktDataStream<Event>, other: DataStream<User>) = view.asFlink().connect(other)")
            .assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `an operator whose result type isn't reified doesn't compile, and its TypeInformation overload does`(compiler: KotlinCompiler) {
        val cases = listOf(
            Triple("R", "fun <R> map(events: FlinktDataStream<Event>, mapper: MapFunction<Event, R>", "events.map(mapper"),
            Triple("K", "fun <K> keyBy(events: FlinktDataStream<Event>, key: KeySelector<Event, K>", "events.keyBy(key"),
            Triple(
                "R",
                "fun <R> process(keyed: FlinktKeyedStream<Event, Long>, fn: KeyedProcessFunction<Long, Event, R>",
                "keyed.process(fn",
            ),
        )
        for ((parameter, declaration, call) in cases) {
            compile(compiler, "$declaration) = $call)")
                .assertFailure("TYPE_PARAMETER_AS_REIFIED", "Cannot use '$parameter' as reified type parameter")
            compile(compiler, "$declaration, type: TypeInformation<$parameter>) = $call, type)").assertSuccess()
        }
    }

    private fun compile(compiler: KotlinCompiler, job: String) = KotlinCompilation(compiler).compile(
        SourceFile("sample/Job.kt", "package sample\n\n$IMPORTS\n\n${job.trimIndent()}\n"),
        SourceFile("sample/Model.kt", MODEL),
        SourceFile("sample/JavaToUser.java", JAVA_TO_USER),
        SourceFile("sample/JavaById.java", JAVA_BY_ID),
        SourceFile("sample/JavaSessionFunction.java", JAVA_SESSION_FUNCTION),
    )

    private companion object {
        val IMPORTS = """
            import io.flinkt.flink.FlinktDataStream
            import io.flinkt.flink.FlinktKeyedStream
            import io.flinkt.flink.FlinktSingleOutputStreamOperator
            import io.flinkt.flink.flinkt
            import io.flinkt.testing.exactTypeOf
            import io.flinkt.testing.shouldBe
            import org.apache.flink.api.common.functions.MapFunction
            import org.apache.flink.api.common.typeinfo.TypeInformation
            import org.apache.flink.api.java.functions.KeySelector
            import org.apache.flink.streaming.api.datastream.DataStream
            import org.apache.flink.streaming.api.datastream.DataStreamSource
            import org.apache.flink.streaming.api.datastream.KeyedStream
            import org.apache.flink.streaming.api.datastream.SideOutputDataStream
            import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
            import org.apache.flink.streaming.api.functions.KeyedProcessFunction
            import org.apache.flink.util.Collector
        """.trimIndent()

        val MODEL = """
            package sample

            import org.apache.flink.api.common.functions.MapFunction
            import org.apache.flink.api.common.functions.RichMapFunction
            import org.apache.flink.api.java.functions.KeySelector
            import org.apache.flink.streaming.api.functions.KeyedProcessFunction
            import org.apache.flink.util.Collector

            data class Event(val id: Long, val name: String)

            data class User(val id: Long, val name: String)

            data class Session(val user: Long, val events: Int)

            class ToUser : MapFunction<Event, User> {
                override fun map(value: Event) = User(value.id, value.name)
            }

            class RichToUser : RichMapFunction<Event, User>() {
                override fun map(value: Event) = User(value.id, value.name)
            }

            class Identity<X> : MapFunction<X, X> {
                override fun map(value: X) = value
            }

            class ById : KeySelector<User, Long> {
                override fun getKey(value: User) = value.id
            }

            class Self<X> : KeySelector<X, X> {
                override fun getKey(value: X) = value
            }

            class SessionFunction : KeyedProcessFunction<Long, User, Session>() {
                override fun processElement(value: User, ctx: Context, out: Collector<Session>) = Unit
            }

            class Passthrough<K, X> : KeyedProcessFunction<K, X, X>() {
                override fun processElement(value: X, ctx: Context, out: Collector<X>) = out.collect(value)
            }
        """.trimIndent()

        val JAVA_TO_USER = """
            package sample;

            import org.apache.flink.api.common.functions.MapFunction;

            public class JavaToUser implements MapFunction<Event, User> {
                @Override
                public User map(Event value) {
                    return new User(value.getId(), value.getName());
                }
            }
        """.trimIndent()

        val JAVA_BY_ID = """
            package sample;

            import org.apache.flink.api.java.functions.KeySelector;

            public class JavaById implements KeySelector<User, Long> {
                @Override
                public Long getKey(User value) {
                    return value.getId();
                }
            }
        """.trimIndent()

        val JAVA_SESSION_FUNCTION = """
            package sample;

            import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
            import org.apache.flink.util.Collector;

            public class JavaSessionFunction extends KeyedProcessFunction<Long, User, Session> {
                @Override
                public void processElement(User value, Context ctx, Collector<Session> out) {}
            }
        """.trimIndent()
    }
}
