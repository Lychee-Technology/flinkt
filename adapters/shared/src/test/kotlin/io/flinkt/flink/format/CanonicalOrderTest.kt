package io.flinkt.flink.format

import io.flinkt.codec.GeneratedCodec
import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.testing.compile.KotlinCompiler
import org.apache.flink.core.memory.DataInputDeserializer
import org.apache.flink.runtime.state.KeyGroupRangeAssignment
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * Canonical field order (docs/persisted-format.md, "Field order"): a codec writes fields in ascending
 * `String.compareTo` order of their names, reads each into a local, then calls the primary constructor once, so an
 * `init` block runs as it does for any construction and sees every argument. Compiled by both pinned compilers.
 *
 * A constructor reorder therefore changes no bytes. It does change a data class's generated `hashCode()`, and with it
 * the key group Flink files a partition key under, which is why the snapshot records constructor order.
 */
class CanonicalOrderTest {
    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a constructor reorder changes no bytes, and moves a partition key's key group`(compiler: KotlinCompiler) {
        val declared = Declarations.load(Declarations.KEY_AB, compiler)
        val reordered = Declarations.load(Declarations.KEY_BA, compiler)
        val before = Declarations.samples(declared).single()
        val after = Declarations.samples(reordered).single()
        assertEquals("Key(a=1, b=2)", before.toString())
        assertEquals("Key(b=2, a=1)", after.toString())

        assertEquals("00 00 00 01 00 00 00 02", codec(declared).write(before).hex())
        assertEquals("00 00 00 01 00 00 00 02", codec(reordered).write(after).hex())

        assertEquals(33, before.hashCode())
        assertEquals(63, after.hashCode())
        assertEquals(29, KeyGroupRangeAssignment.assignToKeyGroup(before, 128))
        assertEquals(44, KeyGroupRangeAssignment.assignToKeyGroup(after, 128))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `decoding calls the constructor once, with every field read, so the init block sees them all`(compiler: KotlinCompiler) {
        for (loader in listOf(Declarations.load(Declarations.KEY_AB, compiler), Declarations.load(Declarations.KEY_BA, compiler))) {
            val codec = codec(loader)
            assertEquals("1 2", codec.deserialize(DataInputDeserializer(bytes("00 00 00 01 00 00 00 02"))).let { "${it.field("a")} ${it.field("b")}" })
            // a = 3 and b = 2 fail the init block's check across both fields, as constructing Key directly would.
            val e = assertThrows<IllegalArgumentException> { codec.deserialize(DataInputDeserializer(bytes("00 00 00 03 00 00 00 02"))) }
            assertEquals("a must not exceed b", e.message)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun codec(loader: ClassLoader): GeneratedCodec<Any> =
        (TypeRegistry.load(loader).resolve(kotlin.jvm.internal.Reflection.typeOf(loader.loadClass("com.example.Key"))) as Resolution.Codec)
            .codec as GeneratedCodec<Any>

    private fun Any.field(name: String): Any = javaClass.getMethod("get" + name.replaceFirstChar(Char::uppercaseChar)).invoke(this)
}
