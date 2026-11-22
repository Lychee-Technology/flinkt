package io.flinkt.ksp

import io.flinkt.codec.GeneratedCodec
import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.schema.RecordSchema
import io.flinkt.testing.compile.CompilationResult
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.Origin
import io.flinkt.testing.compile.SourceFile
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import kotlin.jvm.internal.Reflection
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * The persisted field order KSP computes, its golden bytes and schema metadata, and the `@PersistedFieldOrder`
 * errors it reports (docs/persisted-format.md, "Field order").
 */
class PersistedFieldOrderTest {
    private val processor: List<File> =
        System.getProperty("flinkt.test.kspProcessorClasspath").split(File.pathSeparatorChar).map(::File)

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `default constructor order, sparse keys, renumbering, and constructor reorder with pinned keys`(
        compiler: KotlinCompiler,
    ) {
        val result = compile(
            compiler,
            """
            package com.example

            import io.flinkt.FlinkType
            import io.flinkt.PersistedFieldOrder

            @FlinkType
            data class Account(val id: Long, val name: String, val email: String, val balance: Double, val rating: Float)

            @FlinkType
            data class PinnedAccount(
                @property:PersistedFieldOrder(order = 30) val id: Long,
                @property:PersistedFieldOrder(order = 10) val name: String,
                @property:PersistedFieldOrder(order = 20) val email: String,
                @property:PersistedFieldOrder(order = 99) val balance: Double,
                @property:PersistedFieldOrder(order = 0) val rating: Float,
            )

            // Other keys in the same order, one without a use-site target (PROPERTY target applies automatically).
            @FlinkType
            data class RenumberedAccount(
                @property:PersistedFieldOrder(order = 3) val id: Long,
                @property:PersistedFieldOrder(order = 1) val name: String,
                @property:PersistedFieldOrder(order = 2) val email: String,
                @property:PersistedFieldOrder(order = Int.MAX_VALUE) val balance: Double,
                @PersistedFieldOrder(0) val rating: Float,
            )

            // A constructor reorder of PinnedAccount that keeps its relative persisted order.
            @FlinkType
            data class ReorderedAccount(
                @property:PersistedFieldOrder(order = 0) val rating: Float,
                @property:PersistedFieldOrder(order = 99) val balance: Double,
                @property:PersistedFieldOrder(order = 10) val name: String,
                @property:PersistedFieldOrder(order = 30) val id: Long,
                @property:PersistedFieldOrder(order = 20) val email: String,
            )

            @FlinkType
            data class NestedPinned(
                @property:PersistedFieldOrder(order = 20) val tag: String,
                @property:PersistedFieldOrder(order = 5) val account: PinnedAccount,
            )
            """,
        ).assertSuccess()

        val loader = result.classLoader()
        val registry = TypeRegistry.load(loader)

        fun schema(name: String): RecordSchema =
            assertInstanceOf(RecordSchema::class.java, registry.declaration(loader.loadClass("com.example.$name"))!!.schema)

        @Suppress("UNCHECKED_CAST")
        fun codec(name: String): GeneratedCodec<Any> =
            assertInstanceOf(
                Resolution.Codec::class.java,
                registry.resolve(Reflection.typeOf(loader.loadClass("com.example.$name"))),
            ).codec as GeneratedCodec<Any>

        assertEquals(listOf("id", "name", "email", "balance", "rating"), schema("Account").fields.map { it.name })
        assertEquals(listOf("id", "name", "email", "balance", "rating"), schema("Account").persistedFields.map { it.name })

        val expectedPersisted = listOf("rating", "name", "email", "id", "balance")
        assertEquals(listOf("id", "name", "email", "balance", "rating"), schema("PinnedAccount").fields.map { it.name })
        assertEquals(expectedPersisted, schema("PinnedAccount").persistedFields.map { it.name })

        // Renumbering without changing relative order yields identical constructor and persisted orders.
        assertEquals(schema("PinnedAccount").fields, schema("RenumberedAccount").fields)
        assertEquals(schema("PinnedAccount").persistedFields, schema("RenumberedAccount").persistedFields)

        // Constructor reorder updates constructor fields while preserving persistedFields.
        assertEquals(listOf("rating", "balance", "name", "id", "email"), schema("ReorderedAccount").fields.map { it.name })
        assertEquals(expectedPersisted, schema("ReorderedAccount").persistedFields.map { it.name })

        // Verify exact golden bytes for default constructor order and sparse persisted order.
        val account = loader.loadClass("com.example.Account")
            .getConstructor(Long::class.javaPrimitiveType, String::class.java, String::class.java, Double::class.javaPrimitiveType, Float::class.javaPrimitiveType)
            .newInstance(7L, "Ada", "e", 2.0, -0.0f)
        val accountBytes = serialize(codec("Account"), account)
        assertEquals(
            "00 00 00 00 00 00 00 07 04 41 64 61 02 65 40 00 00 00 00 00 00 00 80 00 00 00",
            hex(accountBytes),
        )
        assertEquals(account, deserialize(codec("Account"), accountBytes))

        val pinned = loader.loadClass("com.example.PinnedAccount")
            .getConstructor(Long::class.javaPrimitiveType, String::class.java, String::class.java, Double::class.javaPrimitiveType, Float::class.javaPrimitiveType)
            .newInstance(7L, "Ada", "e", 2.0, -0.0f)
        val renumbered = loader.loadClass("com.example.RenumberedAccount")
            .getConstructor(Long::class.javaPrimitiveType, String::class.java, String::class.java, Double::class.javaPrimitiveType, Float::class.javaPrimitiveType)
            .newInstance(7L, "Ada", "e", 2.0, -0.0f)
        val reordered = loader.loadClass("com.example.ReorderedAccount")
            .getConstructor(Float::class.javaPrimitiveType, Double::class.javaPrimitiveType, String::class.java, Long::class.javaPrimitiveType, String::class.java)
            .newInstance(-0.0f, 2.0, "Ada", 7L, "e")

        val expectedPinnedHex = "80 00 00 00 04 41 64 61 02 65 00 00 00 00 00 00 00 07 40 00 00 00 00 00 00 00"
        val pinnedBytes = serialize(codec("PinnedAccount"), pinned)
        val renumberedBytes = serialize(codec("RenumberedAccount"), renumbered)
        val reorderedBytes = serialize(codec("ReorderedAccount"), reordered)

        assertEquals(expectedPinnedHex, hex(pinnedBytes))
        assertArrayEquals(pinnedBytes, renumberedBytes)
        assertArrayEquals(pinnedBytes, reorderedBytes)
        assertEquals(pinned, deserialize(codec("PinnedAccount"), pinnedBytes))
        assertEquals(renumbered, deserialize(codec("RenumberedAccount"), renumberedBytes))
        assertEquals(reordered, deserialize(codec("ReorderedAccount"), reorderedBytes))

        // Nested pinned record writes account (order 5) before tag (order 20).
        val nested = loader.loadClass("com.example.NestedPinned")
            .getConstructor(String::class.java, loader.loadClass("com.example.PinnedAccount"))
            .newInstance("hi", pinned)
        val nestedBytes = serialize(codec("NestedPinned"), nested)
        assertEquals("$expectedPinnedHex 03 68 69", hex(nestedBytes))
        assertEquals(nested, deserialize(codec("NestedPinned"), nestedBytes))
    }

    @Test
    fun `KSP rejects partial coverage, negative and repeated keys, and annotations on properties that aren't persisted`() {
        val code = """
            package com.example

            import io.flinkt.FlinkType
            import io.flinkt.PersistedFieldOrder

            @FlinkType
            data class Partial(@property:PersistedFieldOrder(order = 1) val id: Long, val name: String, val email: String)

            @FlinkType
            data class Negative(
                @property:PersistedFieldOrder(order = -1) val id: Long,
                @property:PersistedFieldOrder(order = 1) val name: String,
            )

            @FlinkType
            data class Twice(
                @property:PersistedFieldOrder(order = 10) val id: Long,
                @property:PersistedFieldOrder(order = 10) val name: String,
            )

            @FlinkType
            data class Body(@property:PersistedFieldOrder(order = 0) val id: Long) {
                @property:PersistedFieldOrder(order = 1)
                val display: String get() = "#${'$'}id"
            }

            @FlinkType
            @JvmInline
            value class UserId(@property:PersistedFieldOrder(order = 0) val value: Long)

            data class Plain(@property:PersistedFieldOrder(order = 0) val x: Int)

            @PersistedFieldOrder(order = 0)
            val topLevel: Int = 1
            """.trimIndent()
        val result = compile(KotlinCompiler.BUILD, code).assertFailure("@PersistedFieldOrder")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)

        val lines = code.lines()
        fun line(text: String): Int = lines.indexOfFirst { text in it } + 1
        val rule = "@PersistedFieldOrder orders the primary-constructor properties of a @FlinkType record"
        val expected = setOf(
            line("data class Partial") to "com.example.Partial has @PersistedFieldOrder on id and not on name, email: " +
                "put it on every primary-constructor property, or on none",
            line("order = -1") to "com.example.Negative.id has @PersistedFieldOrder(order = -1): an order is a non-negative Int",
            line("order = 10) val name") to "com.example.Twice.name has @PersistedFieldOrder(order = 10), as " +
                "com.example.Twice.id has: each property's order is unique",
            line("val display") to "@PersistedFieldOrder is on com.example.Body.display, which isn't a " +
                "primary-constructor property, so it isn't persisted. $rule",
            line("value class UserId") to "@PersistedFieldOrder is on com.example.UserId.value, the property of a " +
                "value class, which is persisted as its underlying value and has no field order. $rule",
            line("value class UserId") to "com.example.UserId is a value class, and value classes aren't supported yet (#19)",
            line("data class Plain") to "@PersistedFieldOrder is on com.example.Plain.x, and com.example.Plain isn't a @FlinkType. $rule",
            line("val topLevel") to "@PersistedFieldOrder is on topLevel, which isn't a property of a class. $rule",
        )
        assertEquals(
            expected,
            result.errors.map {
                assertEquals(Origin.KSP, it.origin)
                assertEquals("com/example/Records.kt", it.file)
                it.line to it.message
            }.toSet(),
        )
    }

    private fun compile(compiler: KotlinCompiler, code: String): CompilationResult =
        KotlinCompilation(compiler, kspProcessorClasspath = processor)
            .compile(SourceFile("com/example/Records.kt", code.trimIndent() + "\n"))

    private fun serialize(codec: GeneratedCodec<Any>, value: Any): ByteArray =
        ByteArrayOutputStream().also { codec.serialize(value, DataOutputStream(it)) }.toByteArray()

    private fun deserialize(codec: GeneratedCodec<Any>, bytes: ByteArray): Any {
        val input = DataInputStream(ByteArrayInputStream(bytes))
        return codec.deserialize(input).also { assertEquals(-1, input.read(), "unread trailing bytes") }
    }

    private fun hex(bytes: ByteArray): String = bytes.joinToString(" ") { "%02x".format(it.toInt() and 0xff) }
}
