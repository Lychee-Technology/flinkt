package io.flinkt.ksp

import io.flinkt.testing.compile.CompilationResult
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.Origin
import io.flinkt.testing.compile.SourceFile
import java.io.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * The persisted field order KSP computes, and the `@PersistedFieldOrder` errors it reports (docs/persisted-format.md,
 * "Field order"), with the prototype processor in this module's `persistedOrderPrototype` source set. Evidence for
 * #5: #11 builds the checks into the codec generator, and adds the annotation to flinkt-core, where the snippets
 * declare a stand-in with its approved name and signature.
 */
class PersistedFieldOrderTest {
    private val processor: List<File> =
        System.getProperty("flinkt.test.persistedOrderPrototypeClasspath").split(File.pathSeparatorChar).map(::File)

    private val annotation = SourceFile(
        "io/flinkt/PersistedFieldOrder.kt",
        """
        package io.flinkt

        @Target(AnnotationTarget.PROPERTY)
        @Retention(AnnotationRetention.BINARY)
        annotation class PersistedFieldOrder(val order: Int)
        """.trimIndent() + "\n",
    )

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `the persisted order is the constructor's, or the order of every property's sparse key`(compiler: KotlinCompiler) {
        val result = compile(
            compiler,
            """
            package com.example

            import io.flinkt.FlinkType
            import io.flinkt.PersistedFieldOrder

            @FlinkType
            data class Account(val id: Long, val name: String?, val email: String?, val balance: Double?, val rating: Float)

            @FlinkType
            data class PinnedAccount(
                @property:PersistedFieldOrder(order = 30) val id: Long,
                @property:PersistedFieldOrder(order = 10) val name: String?,
                @property:PersistedFieldOrder(order = 20) val email: String?,
                @property:PersistedFieldOrder(order = 99) val balance: Double?,
                @property:PersistedFieldOrder(order = 0) val rating: Float,
            )

            // Other keys in the same order, one without a use-site target, which Kotlin applies to the property.
            @FlinkType
            data class RenumberedAccount(
                @property:PersistedFieldOrder(order = 3) val id: Long,
                @property:PersistedFieldOrder(order = 1) val name: String?,
                @property:PersistedFieldOrder(order = 2) val email: String?,
                @property:PersistedFieldOrder(order = Int.MAX_VALUE) val balance: Double?,
                @PersistedFieldOrder(0) val rating: Float,
            )

            // A constructor reorder of PinnedAccount that keeps its keys, and so its persisted order.
            @FlinkType
            data class ReorderedAccount(
                @property:PersistedFieldOrder(order = 0) val rating: Float,
                @property:PersistedFieldOrder(order = 99) val balance: Double?,
                @property:PersistedFieldOrder(order = 10) val name: String?,
                @property:PersistedFieldOrder(order = 30) val id: Long,
                @property:PersistedFieldOrder(order = 20) val email: String?,
            )
            """,
        ).assertSuccess()

        val loader = result.classLoader()
        fun persistedOrder(record: String): List<*> {
            val order = loader.loadClass("com.example.${record}PersistedOrder")
            return order.getMethod("getFIELDS").invoke(order.getField("INSTANCE").get(null)) as List<*>
        }
        assertEquals(listOf("id", "name", "email", "balance", "rating"), persistedOrder("Account"))
        for (record in listOf("PinnedAccount", "RenumberedAccount", "ReorderedAccount")) {
            assertEquals(listOf("rating", "name", "email", "id", "balance"), persistedOrder(record), record)
        }
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
        assertEquals(
            setOf(
                line("data class Partial") to "com.example.Partial has @PersistedFieldOrder on id and not on name, email: " +
                    "put it on every primary-constructor property, or on none",
                line("order = -1") to "com.example.Negative.id has @PersistedFieldOrder(order = -1): an order is a non-negative Int",
                line("order = 10) val name") to "com.example.Twice.name has @PersistedFieldOrder(order = 10), as " +
                    "com.example.Twice.id has: each property's order is unique",
                line("val display") to "@PersistedFieldOrder is on com.example.Body.display, which isn't a " +
                    "primary-constructor property, so it isn't persisted. $rule",
                line("value class UserId") to "@PersistedFieldOrder is on com.example.UserId.value, the property of a " +
                    "value class, which is persisted as its underlying value and has no field order. $rule",
                line("data class Plain") to "@PersistedFieldOrder is on com.example.Plain.x, and com.example.Plain isn't a @FlinkType. $rule",
                line("val topLevel") to "@PersistedFieldOrder is on topLevel, which isn't a property of a class. $rule",
            ),
            result.errors.map {
                assertEquals(Origin.KSP, it.origin)
                assertEquals("com/example/Records.kt", it.file)
                it.line to it.message
            }.toSet(),
        )
    }

    private fun compile(compiler: KotlinCompiler, code: String): CompilationResult =
        KotlinCompilation(compiler, kspProcessorClasspath = processor)
            .compile(annotation, SourceFile("com/example/Records.kt", code.trimIndent() + "\n"))
}
