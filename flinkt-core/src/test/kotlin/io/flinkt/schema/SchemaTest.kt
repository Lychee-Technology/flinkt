package io.flinkt.schema

import io.flinkt.assertImmutable
import io.flinkt.example.StandInTypes
import io.flinkt.example.User
import io.flinkt.example.listOfNullableUsers
import io.flinkt.example.userSchema
import io.flinkt.registry.TypeRegistry
import kotlin.reflect.KVariance
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class SchemaTest {
    private val long = SchemaType.ClassType("kotlin.Long")
    private val string = SchemaType.ClassType("kotlin.String")

    @Test
    fun `a schema names its types by strings and keeps their arguments and nullability`() {
        assertEquals("kotlin.collections.List<io.flinkt.example.User?>", listOfNullableUsers.toString())
        assertNotEquals(
            listOfNullableUsers,
            SchemaType.ClassType("kotlin.collections.List", listOf(SchemaTypeArgument.invariant(SchemaType.ClassType("io.flinkt.example.User")))),
        )
        assertNotEquals(
            listOfNullableUsers,
            SchemaType.ClassType(listOfNullableUsers.name, listOfNullableUsers.arguments, isNullable = true),
        )
        assertEquals(
            "kotlin.collections.Map<out kotlin.String, *>",
            SchemaType.ClassType(
                "kotlin.collections.Map",
                listOf(SchemaTypeArgument(KVariance.OUT, string), SchemaTypeArgument.STAR),
            ).toString(),
        )
    }

    @Test
    fun `a record keeps its fields in constructor order, and order is part of the schema`() {
        val reordered = RecordSchema(userSchema.typeName, userSchema.fields.reversed())

        assertEquals(listOf("id", "name"), userSchema.fields.map { it.name })
        assertNotEquals(userSchema, reordered)
        assertEquals(userSchema, RecordSchema("io.flinkt.example.User", listOf(FieldSchema("id", long), FieldSchema("name", string))))
        assertEquals(userSchema.hashCode(), RecordSchema("io.flinkt.example.User", userSchema.fields).hashCode())
    }

    @Test
    fun `a generic declaration refers to its own type parameters only`() {
        val envelope = RecordSchema(
            "com.example.Envelope",
            listOf(
                FieldSchema("payload", SchemaType.TypeParameter("T")),
                FieldSchema("history", SchemaType.ClassType("kotlin.collections.List", listOf(SchemaTypeArgument.invariant(SchemaType.TypeParameter("T", isNullable = true))))),
            ),
            typeParameters = listOf("T"),
        )
        assertEquals("record com.example.Envelope<T>(payload: T, history: kotlin.collections.List<T?>)", envelope.toString())

        assertMessage("com.example.Envelope refers to a type parameter U it doesn't declare") {
            RecordSchema("com.example.Envelope", listOf(FieldSchema("payload", SchemaType.TypeParameter("U"))), listOf("T"))
        }
        assertMessage("com.example.Box has two type parameters named T") {
            ValueClassSchema("com.example.Box", FieldSchema("value", SchemaType.TypeParameter("T")), listOf("T", "T"))
        }
    }

    @Test
    fun `enum constants and sealed subtypes carry positive, unique IDs`() {
        val color = EnumSchema("com.example.Color", listOf(EnumConstantSchema("RED", 1), EnumConstantSchema("BLUE", 7)))
        assertEquals("enum com.example.Color { RED = 1, BLUE = 7 }", color.toString())
        assertTrue(color.typeParameters.isEmpty())

        assertMessage("The ID of RED is 0, but IDs are positive") { EnumConstantSchema("RED", 0) }
        assertMessage("com.example.Color has two constants with ID 1") {
            EnumSchema("com.example.Color", listOf(EnumConstantSchema("RED", 1), EnumConstantSchema("GREEN", 1)))
        }
        assertMessage("com.example.Color has two constants named RED") {
            EnumSchema("com.example.Color", listOf(EnumConstantSchema("RED", 1), EnumConstantSchema("RED", 2)))
        }

        val created = SchemaType.ClassType("com.example.UserCreated")
        val event = SealedSchema("com.example.Event", listOf(SealedSubtypeSchema(1, created), SealedSubtypeSchema(2, SchemaType.ClassType("com.example.Shutdown"))))
        assertEquals("sealed com.example.Event { 1 = com.example.UserCreated, 2 = com.example.Shutdown }", event.toString())
        assertMessage("com.example.Event has two subtypes with ID 1") {
            SealedSchema("com.example.Event", listOf(SealedSubtypeSchema(1, created), SealedSubtypeSchema(1, SchemaType.ClassType("com.example.Other"))))
        }
        assertMessage("The ID of com.example.UserCreated is -1, but IDs are positive") { SealedSubtypeSchema(-1, created) }
        assertMessage("A sealed subtype isn't nullable") {
            SealedSubtypeSchema(1, SchemaType.ClassType("com.example.UserCreated", isNullable = true))
        }
    }

    @Test
    fun `a value class keeps its own identity beside its underlying type`() {
        val userId = ValueClassSchema("com.example.UserId", FieldSchema("value", long))
        val orderId = ValueClassSchema("com.example.OrderId", FieldSchema("value", long))

        assertNotEquals(userId, orderId)
        assertEquals("value class com.example.UserId(value: kotlin.Long)", userId.toString())
        assertEquals("object com.example.Shutdown", ObjectSchema("com.example.Shutdown").toString())
    }

    @Test
    fun `a schema's lists are copies that can't be changed after validation`() {
        val fields = mutableListOf(FieldSchema("id", long), FieldSchema("history", listOfNullableUsers))
        val typeParameters = mutableListOf("T", "U")
        val record = RecordSchema("com.example.Pair", fields, typeParameters)
        fields.clear()
        typeParameters.clear()
        assertEquals("record com.example.Pair<T, U>(id: kotlin.Long, history: kotlin.collections.List<io.flinkt.example.User?>)", record.toString())

        assertImmutable(record.fields)
        assertImmutable(record.typeParameters)
        assertImmutable((record.fields[1].type as SchemaType.ClassType).arguments)
        assertImmutable(listOfNullableUsers.arguments)
        assertImmutable(ValueClassSchema("com.example.Box", FieldSchema("value", SchemaType.TypeParameter("T")), listOf("T")).typeParameters)
        assertImmutable(EnumSchema("com.example.Color", listOf(EnumConstantSchema("RED", 1), EnumConstantSchema("BLUE", 2))).constants)
        val created = SealedSubtypeSchema(1, SchemaType.ClassType("com.example.Created", listOf(SchemaTypeArgument.invariant(SchemaType.TypeParameter("T")))))
        val event = SealedSchema("com.example.Event", listOf(created, SealedSubtypeSchema(2, SchemaType.ClassType("com.example.Shutdown"))), listOf("T"))
        assertImmutable(event.subtypes)
        assertImmutable(event.typeParameters)
        assertImmutable(event.subtypes[0].type.arguments)
        assertEquals("sealed com.example.Event<T> { 1 = com.example.Created<T>, 2 = com.example.Shutdown }", event.toString())
    }

    @Test
    fun `a registered schema can't be changed through the registry`() {
        val registry = TypeRegistry.of(StandInTypes())
        val schema = registry.declaration(User::class)!!.schema as RecordSchema

        assertImmutable(schema.fields)
        assertEquals(userSchema, registry.declaration(User::class)!!.schema)
        assertEquals(listOf("id", "name"), schema.fields.map { it.name })
        assertImmutable(registry.declarations)
        assertImmutable(registry.modules)
    }

    private fun assertMessage(expected: String, block: () -> Unit) {
        val error = assertThrows<IllegalArgumentException>(block)
        assertTrue(expected in error.message.orEmpty(), "Expected \"$expected\" in: ${error.message}")
    }
}
