package io.flinkt.ksp

import io.flinkt.ksp.KspTestSupport.compile
import io.flinkt.ksp.KspTestSupport.compileSingle
import io.flinkt.testing.compile.CompilationResult
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.Origin
import io.flinkt.testing.compile.SourceFile
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Negative KSP compile tests covering every error row of the persisted-state rule table and every unsupported
 * declaration and field type in the first slice.
 */
class KspDiagnosticsTest {
    @Test
    fun `rejects body properties with backing fields, lateinit, delegated properties, and inherited state`() {
        val code = """
            package com.example

            import io.flinkt.FlinkType
            import kotlin.properties.Delegates

            @FlinkType
            data class BodyVal(val id: Long) {
                val createdAt: Long = 1L
            }

            @FlinkType
            data class BodyVar(val id: Long) {
                var count: Int = 0
            }

            @FlinkType
            data class BodyLateinit(val id: Long) {
                lateinit var tag: String
            }

            @FlinkType
            data class DelegatedLazy(val id: Long) {
                val cached: String by lazy { id.toString() }
            }

            @FlinkType
            data class DelegatedObservable(val id: Long) {
                var observed: Int by Delegates.observable(0) { _, _, _ -> }
            }

            open class StatefulBase(val baseId: Long)

            @FlinkType
            data class InheritsCtorState(val id: Long) : StatefulBase(1L)

            abstract class BodyStatefulBase {
                val baseCreated: Long = 1L
            }

            @FlinkType
            data class InheritsBodyState(val id: Long) : BodyStatefulBase()
        """.trimIndent()

        val result = compileSingle(code).assertFailure("backing field", "delegated", "inherits stored state")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
        assertAllKspErrors(result)

        assertErrorContaining(result, "com.example.BodyVal.createdAt", "backing field", "computed getter", "primary-constructor property")
        assertErrorContaining(result, "com.example.BodyVar.count", "backing field", "computed getter", "primary-constructor property")
        assertErrorContaining(result, "com.example.BodyLateinit.tag", "backing field", "computed getter", "primary-constructor property")
        assertErrorContaining(result, "com.example.DelegatedLazy.cached", "delegated property")
        assertErrorContaining(result, "com.example.DelegatedObservable.observed", "delegated property")
        assertErrorContaining(result, "com.example.InheritsCtorState", "com.example.StatefulBase.baseId")
        assertErrorContaining(result, "com.example.InheritsBodyState", "com.example.BodyStatefulBase.baseCreated")
    }

    @Test
    fun `rejects inaccessible constructors, properties, and classes`() {
        val code = """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class PrivateCtor private constructor(val id: Long)

            @FlinkType
            data class PrivateProp(private val secret: String)

            @FlinkType
            private data class PrivateClass(val id: Long)

            private class PrivateOuter {
                @FlinkType
                data class NestedInPrivate(val id: Long)
            }

            open class OpenOuter {
                @FlinkType
                protected data class ProtectedNested(val id: Long)
            }
        """.trimIndent()

        val result = compileSingle(code).assertFailure("private")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
        assertAllKspErrors(result)

        assertErrorContaining(result, "com.example.PrivateCtor", "private primary constructor")
        assertErrorContaining(result, "com.example.PrivateProp.secret", "private getter")
        assertErrorContaining(result, "com.example.PrivateClass", "is private")
        assertErrorContaining(result, "com.example.PrivateOuter.NestedInPrivate", "PrivateOuter", "is private")
        assertErrorContaining(result, "com.example.OpenOuter.ProtectedNested", "is protected")
    }

    @Test
    fun `rejects declarations that aren't non-generic data classes`() {
        val code = """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            class PlainClass(val id: Long)

            @FlinkType
            enum class Color { RED, GREEN }

            @FlinkType
            sealed interface Event

            @FlinkType
            sealed class SealedBase

            @FlinkType
            @JvmInline
            value class UserId(val value: Long)

            @FlinkType
            object PlainObj

            @FlinkType
            data object DataObj

            @FlinkType
            interface SomeInterface

            @FlinkType
            annotation class SomeAnnotation

            @FlinkType
            data class Envelope<T>(val payload: T)
        """.trimIndent()

        val result = compileSingle(code).assertFailure("data class")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
        assertAllKspErrors(result)

        assertErrorContaining(result, "com.example.PlainClass", "isn't a data class")
        assertErrorContaining(result, "com.example.Color", "enum")
        assertErrorContaining(result, "com.example.Event", "sealed")
        assertErrorContaining(result, "com.example.SealedBase", "sealed")
        assertErrorContaining(result, "com.example.UserId", "value class")
        assertErrorContaining(result, "com.example.PlainObj", "object")
        assertErrorContaining(result, "com.example.DataObj", "object")
        assertErrorContaining(result, "com.example.SomeInterface", "interface")
        assertErrorContaining(result, "com.example.SomeAnnotation", "annotation class")
        assertErrorContaining(result, "com.example.Envelope<T>", "generic")
    }

    @Test
    fun `rejects every unsupported field type naming declaration, property, and type`() {
        val code = """
            package com.example

            import io.flinkt.FlinkType
            import java.math.BigDecimal
            import java.util.UUID

            typealias MaybeName = String?

            @FlinkType
            data class Address(val city: String)

            data class Unannotated(val x: Int)

            @JvmInline
            value class UserId(val value: Long)

            enum class Color { RED }

            sealed interface Event

            data object Shutdown

            @FlinkType
            data class Envelope<T>(val payload: T)

            @FlinkType
            data class BadFields(
                val nullableInt: Int?,
                val nullableString: String?,
                val nullableAddress: Address?,
                val nullableAlias: MaybeName,
                val listField: List<String>,
                val mapField: Map<String, Long>,
                val mutableListField: MutableList<String>,
                val mutableMapField: MutableMap<String, Long>,
                val setField: Set<Int>,
                val collectionField: Collection<String>,
                val iterableField: Iterable<String>,
                val arrayField: Array<String>,
                val intArrayField: IntArray,
                val valueClassField: UserId,
                val enumField: Color,
                val sealedField: Event,
                val objectField: Shutdown,
                val genericField: Envelope<String>,
                val unannotatedField: Unannotated,
                val uuidField: UUID,
                val decimalField: BigDecimal,
                val anyField: Any,
            )
        """.trimIndent()

        val result = compileSingle(code).assertFailure("BadFields")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
        assertAllKspErrors(result)

        val expectedFields = listOf(
            "com.example.BadFields.nullableInt: kotlin.Int?" to "nullable",
            "com.example.BadFields.nullableString: kotlin.String?" to "nullable",
            "com.example.BadFields.nullableAddress: com.example.Address?" to "nullable",
            "com.example.BadFields.nullableAlias: kotlin.String?" to "nullable",
            "com.example.BadFields.listField: kotlin.collections.List<kotlin.String>" to "collections",
            "com.example.BadFields.mapField: kotlin.collections.Map<kotlin.String, kotlin.Long>" to "collections",
            "com.example.BadFields.mutableListField: kotlin.collections.MutableList<kotlin.String>" to "collection",
            "com.example.BadFields.mutableMapField: kotlin.collections.MutableMap<kotlin.String, kotlin.Long>" to "collection",
            "com.example.BadFields.setField: kotlin.collections.Set<kotlin.Int>" to "collection",
            "com.example.BadFields.collectionField: kotlin.collections.Collection<kotlin.String>" to "collection",
            "com.example.BadFields.iterableField: kotlin.collections.Iterable<kotlin.String>" to "collection",
            "com.example.BadFields.arrayField: kotlin.Array<kotlin.String>" to "array",
            "com.example.BadFields.intArrayField: kotlin.IntArray" to "array",
            "com.example.BadFields.valueClassField: com.example.UserId" to "value class",
            "com.example.BadFields.enumField: com.example.Color" to "enum",
            "com.example.BadFields.sealedField: com.example.Event" to "sealed",
            "com.example.BadFields.objectField: com.example.Shutdown" to "object",
            "com.example.BadFields.genericField: com.example.Envelope<kotlin.String>" to "generic",
            "com.example.BadFields.unannotatedField: com.example.Unannotated" to "@FlinkType",
            "com.example.BadFields.uuidField: java.util.UUID" to "@FlinkType",
            "com.example.BadFields.decimalField: java.math.BigDecimal" to "@FlinkType",
            "com.example.BadFields.anyField: kotlin.Any" to "@FlinkType",
        )
        for ((prefix, reason) in expectedFields) {
            assertErrorContaining(result, prefix, reason)
        }
    }

    @Test
    fun `rejects recursive @FlinkType data classes directly and mutually`() {
        val code = """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class DirectLoop(val next: DirectLoop)

            @FlinkType
            data class NodeA(val b: NodeB)

            @FlinkType
            data class NodeB(val a: NodeA)
        """.trimIndent()

        val result = compileSingle(code).assertFailure("recursive cycle")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
        assertAllKspErrors(result)

        assertErrorContaining(result, "com.example.DirectLoop.next", "com.example.DirectLoop -> com.example.DirectLoop")
        assertErrorContaining(result, "com.example.NodeA.b", "com.example.NodeA -> com.example.NodeB -> com.example.NodeA")
        assertErrorContaining(result, "com.example.NodeB.a", "com.example.NodeB -> com.example.NodeA -> com.example.NodeB")
    }

    @Test
    fun `rejects cross-module @FlinkType field references explicitly until #24`() {
        val dependency = KotlinCompilation(KotlinCompiler.BUILD).compile(
            SourceFile(
                "com/external/ExternalAddress.kt",
                """
                package com.external

                import io.flinkt.FlinkType

                @FlinkType
                data class ExternalAddress(val city: String)
                """.trimIndent() + "\n",
            ),
        ).assertSuccess()

        val result = compile(
            KotlinCompiler.BUILD,
            SourceFile(
                "com/example/LocalUser.kt",
                """
                package com.example

                import com.external.ExternalAddress
                import io.flinkt.FlinkType

                @FlinkType
                data class LocalUser(val id: Long, val address: ExternalAddress)
                """.trimIndent() + "\n",
            ),
            extraClasspath = listOf(dependency.classesDir),
        ).assertFailure("com.example.LocalUser.address", "com.external.ExternalAddress", "another module")
        assertEquals(CompilationResult.KSP_ERROR, result.exitCode)
        assertAllKspErrors(result)
    }

    private fun assertAllKspErrors(result: CompilationResult) {
        assertTrue(result.errors.isNotEmpty(), "Expected at least one error")
        for (error in result.errors) {
            assertEquals(Origin.KSP, error.origin, "Expected KSP diagnostic, got: $error")
        }
    }

    private fun assertErrorContaining(result: CompilationResult, vararg parts: String) {
        val matched = result.errors.any { err -> parts.all { part -> part in err.message } }
        assertTrue(
            matched,
            "Expected a KSP error containing ${parts.joinToString { "\"$it\"" }}.\nActual errors:\n" +
                result.errors.joinToString("\n") { "  $it" },
        )
    }
}
