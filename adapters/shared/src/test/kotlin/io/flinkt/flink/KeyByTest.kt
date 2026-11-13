package io.flinkt.flink

import io.flinkt.FlinkType
import io.flinkt.flink.StreamGraphs.transformations
import io.flinkt.types.UnsupportedTypeException
import java.math.BigDecimal
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * The view's interim key policy, until Flinkt's key rules are implemented (#32): keyBy accepts the non-null built-ins
 * that typeInfo() resolves to Flink's own BasicTypeInfo, and refuses every other key type before it calls Flink.
 */
class KeyByTest {
    private val env = StreamExecutionEnvironment.createLocalEnvironment(2)
    private val events = env.fromSequence(1, 100).flinkt()

    @Test
    fun `keyBy builds the keyed stream for each built-in key type, with Flink's BasicTypeInfo for it`() {
        val keyTypes: List<Pair<TypeInformation<*>, () -> FlinktKeyedStream<Long, *>>> = listOf(
            BasicTypeInfo.BOOLEAN_TYPE_INFO to { events.keyBy { it % 2 == 0L } },
            BasicTypeInfo.BYTE_TYPE_INFO to { events.keyBy { it.toByte() } },
            BasicTypeInfo.SHORT_TYPE_INFO to { events.keyBy { it.toShort() } },
            BasicTypeInfo.INT_TYPE_INFO to { events.keyBy { it.toInt() } },
            BasicTypeInfo.LONG_TYPE_INFO to { events.keyBy { it } },
            BasicTypeInfo.FLOAT_TYPE_INFO to { events.keyBy { it.toFloat() } },
            BasicTypeInfo.DOUBLE_TYPE_INFO to { events.keyBy { it.toDouble() } },
            BasicTypeInfo.CHAR_TYPE_INFO to { events.keyBy { 'a' + (it % 26).toInt() } },
            BasicTypeInfo.STRING_TYPE_INFO to { events.keyBy { it.toString() } },
        )
        for ((keyType, keyBy) in keyTypes) {
            assertSame(keyType, keyBy().asFlink().keyType, "$keyType")
        }
    }

    @Test
    fun `the overload that takes a TypeInformation accepts Flink's BasicTypeInfo for each built-in key type`() {
        assertSame(BasicTypeInfo.LONG_TYPE_INFO, events.keyBy(KeySelector { it }, Types.LONG).asFlink().keyType)
        assertSame(BasicTypeInfo.STRING_TYPE_INFO, events.keyBy(KeySelector { it.toString() }, Types.STRING).asFlink().keyType)
        assertSame(BasicTypeInfo.CHAR_TYPE_INFO, events.keyBy(KeySelector { 'a' }, BasicTypeInfo.getInfoFor(Char::class.javaObjectType)).asFlink().keyType)
    }

    @Test
    fun `a key type from Java, a platform type, is accepted as its non-null type`() {
        // The lambda returns String!, the result type of a Java method.
        assertSame(BasicTypeInfo.STRING_TYPE_INFO, events.keyBy { java.lang.String.valueOf(it) }.asFlink().keyType)
    }

    @Test
    fun `keyBy refuses a nullable, a collection or a data-class key, naming its Kotlin type, before it calls Flink`() {
        val before = transformations(env)
        assertEquals(refusal("kotlin.String?"), refused { events.keyBy { it.toString().takeIf { text -> text.isNotEmpty() } } })
        assertEquals(refusal("kotlin.collections.List<kotlin.String>"), refused { events.keyBy { listOf(it.toString()) } })
        assertEquals(refusal("io.flinkt.flink.KeyByTest.UserKey"), refused { events.keyBy { UserKey(it) } })
        assertSameElements(before, transformations(env))
    }

    @Test
    fun `keyBy refuses every other key type, whether or not typeInfo resolves it`() {
        val refusals: Map<String, () -> Any> = mapOf(
            "kotlin.Int?" to { events.keyBy { it.toInt().takeIf { n -> n > 0 } } },
            "io.flinkt.flink.KeyByTest.Generated" to { events.keyBy { Generated(it) } },
            "io.flinkt.flink.KeyByTest.UserId" to { events.keyBy { UserId(it) } },
            "io.flinkt.flink.KeyByTest.Priority" to { events.keyBy { Priority.HIGH } },
            "io.flinkt.flink.KeyByTest.Shape" to { events.keyBy<Shape> { Shape.Circle(it) } },
            "io.flinkt.flink.KeyByTest.Origin" to { events.keyBy { Origin } },
            "kotlin.collections.Map<kotlin.String, kotlin.Long>" to { events.keyBy { mapOf("id" to it) } },
            "kotlin.Pair<kotlin.Long, kotlin.Long>" to { events.keyBy { it to it } },
            // Flink has a BasicTypeInfo for it, but it isn't one of the built-ins.
            "java.math.BigDecimal" to { events.keyBy { BigDecimal.valueOf(it) } },
        )
        for ((type, keyBy) in refusals) {
            assertEquals(refusal(type), refused(keyBy), type)
        }
    }

    @Test
    fun `keyBy checks the key type before it looks up its TypeInformation`() {
        // typeInfo() refuses String? for its own reason, and keyBy gives the key policy's.
        val typeInfo = assertThrows<UnsupportedTypeException> { typeInfo<String?>() }
        assertEquals("kotlin.String?", typeInfo.canonicalType)
        assertEquals(refusal("kotlin.String?"), refused { events.keyBy<String?> { null } })
    }

    @Test
    fun `the overload that takes a TypeInformation refuses one outside the built-ins, naming it, before it calls Flink`() {
        val before = transformations(env)
        val refusals: List<Pair<String, () -> Any>> = listOf(
            "List<String>" to { events.keyBy(KeySelector { listOf(it.toString()) }, Types.LIST(Types.STRING)) },
            "BigDecimal" to { events.keyBy(KeySelector { BigDecimal.valueOf(it) }, BasicTypeInfo.BIG_DEC_TYPE_INFO) },
            "GenericType<java.lang.String>" to { events.keyBy(KeySelector { it.toString() }, GenericTypeInfo(String::class.java)) },
        )
        for ((name, keyBy) in refusals) {
            val e = assertThrows<IllegalArgumentException>(name) { keyBy() }
            assertEquals(
                "keyBy doesn't accept the key type $name yet. Until Flinkt's key rules are implemented, the view's " +
                    "keyBy accepts only Flink's BasicTypeInfo for a non-null Boolean, Byte, Short, Int, Long, Float, " +
                    "Double, Char or String key, the one typeInfo() gives for it, and keys of other types wait for " +
                    "those rules. To key the stream by Flink's own rules instead, call asFlink().keyBy(...).",
                e.message,
            )
        }
        assertSameElements(before, transformations(env))
    }

    @Test
    fun `Flink's own keyBy stays reachable through asFlink`() {
        val keyed = events.asFlink().keyBy(KeySelector { listOf(it.toString()) }, Types.LIST(Types.STRING))
        assertEquals(Types.LIST(Types.STRING), keyed.keyType)
    }

    /** The message of a refused [type], given as its canonical Kotlin type. */
    private fun refusal(type: String): String =
        "keyBy doesn't accept $type as a key type yet. Until Flinkt's key rules are implemented, the view's keyBy " +
            "accepts only a non-null Boolean, Byte, Short, Int, Long, Float, Double, Char or String key, and keys of " +
            "other types wait for those rules. To key the stream by Flink's own rules instead, call asFlink().keyBy(...)."

    /** The message of the key policy's refusal, which isn't typeInfo()'s `UnsupportedTypeException`. */
    private fun refused(keyBy: () -> Any): String? {
        val e = assertThrows<IllegalArgumentException> { keyBy() }
        assertFalse(e is UnsupportedTypeException, "$e")
        return e.message
    }

    private fun assertSameElements(expected: List<*>, actual: List<*>) {
        assertEquals(expected.size, actual.size, "$actual")
        expected.zip(actual).forEach { (e, a) -> assertSame(e, a) }
    }

    data class UserKey(val id: Long)

    @FlinkType
    data class Generated(val id: Long)

    @JvmInline
    value class UserId(val value: Long)

    enum class Priority { HIGH }

    sealed interface Shape {
        data class Circle(val radius: Long) : Shape
    }

    object Origin
}
