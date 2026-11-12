package io.flinkt.registry

import io.flinkt.codec.CodecContext
import io.flinkt.codec.GeneratedCodec
import io.flinkt.example.Color
import io.flinkt.example.Contact
import io.flinkt.example.Envelope
import io.flinkt.example.Event
import io.flinkt.example.IntCodec
import io.flinkt.example.Loop
import io.flinkt.example.Order
import io.flinkt.example.OrderCodec
import io.flinkt.example.Plain
import io.flinkt.example.Shutdown
import io.flinkt.example.StandInBuiltIns
import io.flinkt.example.StandInTypes
import io.flinkt.example.StringCodec
import io.flinkt.example.Unregistered
import io.flinkt.example.User
import io.flinkt.example.UserCodec
import io.flinkt.example.UserCodecFactory
import io.flinkt.example.UserId
import io.flinkt.schema.RecordSchema
import io.flinkt.types.BuiltInKind
import io.flinkt.types.UnsupportedTypeException
import io.flinkt.types.kTypeOf
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/** Supported resolution: built-ins and non-generic generated records resolve, and every other shape fails explicitly. */
class ResolutionTest {
    private val registry = TypeRegistry.of(StandInTypes(), StandInBuiltIns())

    private val user = User::class.java.name

    @Test
    fun `a built-in resolves to the codec registered for it`() {
        val codec = registry.codec<Int>()

        assertSame(IntCodec, codec)
        assertEquals(42, roundTrip(codec, 42))
    }

    @Test
    fun `a generated record resolves to its codec`() {
        val codec = registry.codec<User>()

        assertSame(UserCodec, codec)
        assertEquals(User(7, "Ada"), roundTrip(codec, User(7, "Ada")))
    }

    @Test
    fun `a record's codec takes its nested codecs once, when it's created`() {
        val before = UserCodecFactory.created.get()
        val codec = registry.codec<Order>()
        assertEquals(before + 1, UserCodecFactory.created.get())

        assertInstanceOf(OrderCodec::class.java, codec)
        repeat(3) { i -> assertEquals(Order(i.toLong(), User(1, "Ada")), roundTrip(codec, Order(i.toLong(), User(1, "Ada")))) }
        assertEquals(before + 1, UserCodecFactory.created.get(), "serializing looked no codec up")
    }

    @Test
    fun `a codec context can't be used after its codec is created`() {
        var kept: CodecContext? = null
        val leaky = TypeRegistry.of(
            object : GeneratedTypeModule {
                override fun register(registry: MutableTypeRegistry) {
                    registry.register(Plain::class, RecordSchema(Plain::class.java.name, emptyList())) { context ->
                        kept = context
                        PlainCodec
                    }
                    registry.registerBuiltIn(BuiltInKind.INT) { IntCodec }
                }
            },
        )
        leaky.codec<Plain>()

        val error = assertThrows<IllegalStateException> { kept!!.codec(typeOf<Int>()) }
        assertContains(error.message, "used after its codec was created")
    }

    @Test
    fun `a platform type from Java resolves as the non-null type`() {
        assertSame(StringCodec, registry.codec(kTypeOf(System.getProperty("java.version"))))
    }

    @Test
    fun `nullable, collection and generic types are modeled but fail with their canonical type`() {
        assertUnsupported(typeOf<User?>(), "Flinkt can't resolve $user?: nullable types aren't supported yet")
        assertUnsupported(
            typeOf<List<User>>(),
            "Flinkt can't resolve kotlin.collections.List<$user>: List and Map aren't supported yet",
        )
        assertUnsupported(
            typeOf<Envelope<List<User?>>>(),
            "Flinkt can't resolve ${Envelope::class.java.name}<kotlin.collections.List<$user?>>: " +
                "generic classes aren't supported yet",
        )
        assertUnsupported(typeOf<Int?>(), "Flinkt can't resolve kotlin.Int?: nullable types aren't supported yet")
    }

    @Test
    fun `every other modeled shape fails explicitly`() {
        assertUnsupported(typeOf<UserId>(), "${UserId::class.java.name}: value classes aren't supported yet")
        assertUnsupported(typeOf<Color>(), "${Color::class.java.name}: enums aren't supported yet")
        assertUnsupported(typeOf<Event>(), "${Event::class.java.name}: sealed types aren't supported yet")
        assertUnsupported(typeOf<Shutdown>(), "${Shutdown::class.java.name}: objects aren't supported yet")
        assertUnsupported(
            typeOf<MutableList<User>>(),
            "kotlin.collections.MutableList<$user>: of the collection types, Flinkt models only the read-only List and Map",
        )
        assertUnsupported(typeOf<Set<Int>>(), "kotlin.collections.Set<kotlin.Int>: of the collection types")
        assertUnsupported(typeOf<IntArray>(), "kotlin.IntArray: of the collection types")
        assertUnsupported(typeOf<Long>(), "kotlin.Long: no GeneratedTypeModule that this registry found registers a codec for it")
        assertUnsupported(typeOf<Any>(), "kotlin.Any: it isn't a built-in type, and it isn't annotated with @FlinkType")
        assertUnsupported(typeOf<Plain>(), "${Plain::class.java.name}: it isn't a built-in type, and it isn't annotated with @FlinkType")
        assertUnsupported(
            typeOf<Unregistered>(),
            "${Unregistered::class.java.name}: it's annotated with @FlinkType, but no GeneratedTypeModule that this " +
                "registry found registers it",
        )
        assertUnsupported(typeOf<List<Nothing>>().arguments.single().type!!, "kotlin.Nothing: it isn't a built-in type")
    }

    @Test
    fun `a type parameter fails, because its type isn't known`() {
        assertUnsupported(typeParameter<User>(), "Flinkt can't resolve T: it's a type parameter, so its type isn't known here")
    }

    @Test
    fun `a record whose field can't be resolved fails with both types`() {
        assertUnsupported(
            typeOf<Contact>(),
            "Flinkt can't resolve ${Contact::class.java.name}: it contains $user?, which Flinkt can't resolve: " +
                "nullable types aren't supported yet",
        )
    }

    @Test
    fun `a record that contains itself fails instead of recursing`() {
        val loop = Loop::class.java.name
        assertUnsupported(typeOf<Loop>(), "its codec needs itself ($loop -> $loop), and recursive types aren't supported yet")
    }

    @Test
    fun `a built-in fails when no module registers its codec`() {
        val error = assertThrows<UnsupportedTypeException> { TypeRegistry.of(StandInTypes()).codec<Int>() }
        assertEquals("kotlin.Int", error.canonicalType)
    }

    @Test
    fun `a model from another registry resolves only if this registry has its declaration`() {
        val model = registry.model<User>()

        assertSame(UserCodec, TypeRegistry.of(StandInTypes()).codec(model))
        val error = assertThrows<UnsupportedTypeException> { TypeRegistry.of(StandInBuiltIns()).codec(model) }
        assertContains(error.message, "no GeneratedTypeModule that this registry found registers it")
    }

    private fun assertUnsupported(type: KType, expected: String) {
        val error = assertThrows<UnsupportedTypeException> { registry.codec(type) }
        assertContains(error.message, expected)
        assertEquals(registry.model(type).toString(), error.canonicalType)
    }

    private fun <T> typeParameter(): KType = typeOf<List<T>>().arguments.single().type!!

    private object PlainCodec : GeneratedCodec<Plain> {
        override fun serialize(value: Plain, output: java.io.DataOutput): Unit = output.writeLong(value.id)

        override fun deserialize(input: java.io.DataInput): Plain = Plain(input.readLong())

        override fun copy(value: Plain): Plain = value
    }
}

internal fun assertContains(actual: String?, expected: String) {
    assertTrue(actual != null && expected in actual, "Expected \"$expected\" in:\n$actual")
}

internal fun <T> roundTrip(codec: GeneratedCodec<T>, value: T): T {
    val bytes = ByteArrayOutputStream().also { codec.serialize(value, DataOutputStream(it)) }.toByteArray()
    val input = DataInputStream(ByteArrayInputStream(bytes))
    return codec.deserialize(input).also { assertEquals(-1, input.read(), "the codec read every byte it wrote") }
}

