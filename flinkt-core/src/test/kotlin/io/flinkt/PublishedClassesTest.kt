package io.flinkt

import io.flinkt.testing.ClassFiles
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class PublishedClassesTest {
    @Test
    fun `classes target Java 8 and carry Kotlin metadata 2_4_0`() {
        assertEquals(52, ClassFiles.majorVersion(FlinkType::class.java))
        assertEquals("2.4.0", ClassFiles.kotlinMetadataVersion(FlinkType::class.java))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `user code compiles against flinkt-core`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(
                """
                import io.flinkt.FlinkType

                @FlinkType
                data class User(val id: Long, val name: String)
                """,
            )
            .assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `code like the generated code compiles against the SPI`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(
                """
                import io.flinkt.FlinkType
                import io.flinkt.codec.GeneratedCodec
                import io.flinkt.codec.codec
                import io.flinkt.registry.GeneratedTypeModule
                import io.flinkt.registry.MutableTypeRegistry
                import io.flinkt.schema.FieldSchema
                import io.flinkt.schema.RecordSchema
                import io.flinkt.schema.SchemaType
                import java.io.DataInput
                import java.io.DataOutput

                @FlinkType
                data class User(val id: Long)

                @FlinkType
                data class Order(val user: User)

                class OrderCodec(private val user: GeneratedCodec<User>) : GeneratedCodec<Order> {
                    override fun serialize(value: Order, output: DataOutput) = user.serialize(value.user, output)
                    override fun deserialize(input: DataInput): Order = Order(user.deserialize(input))
                    override fun copy(value: Order): Order = Order(user.copy(value.user))
                }

                class ModelTypes : GeneratedTypeModule {
                    override fun register(registry: MutableTypeRegistry) {
                        val schema = RecordSchema("Order", listOf(FieldSchema("user", SchemaType.ClassType("User"))))
                        registry.register(Order::class, schema) { context -> OrderCodec(context.codec()) }
                    }
                }
                """,
            )
            .assertSuccess()
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `code like an adapter's handles every resolution without a fallback branch`(compiler: KotlinCompiler) {
        // The when is an expression, so it compiles only if BuiltIn and Codec are every resolution there is.
        KotlinCompilation(compiler)
            .compile(
                """
                import io.flinkt.registry.Resolution
                import io.flinkt.registry.TypeRegistry
                import io.flinkt.registry.resolve

                inline fun <reified T> serializerOf(registry: TypeRegistry): String =
                    when (val resolution = registry.resolve<T>()) {
                        is Resolution.BuiltIn -> "Flink's own serializer for " + resolution.kind.jvmClass.name
                        is Resolution.Codec -> "an adapter serializer over " + resolution.codec
                    }
                """,
            )
            .assertSuccess()
    }
}
