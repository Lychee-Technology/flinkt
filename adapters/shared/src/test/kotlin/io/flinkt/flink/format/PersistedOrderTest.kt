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
 * Persisted field order (docs/persisted-format.md, "Field order"): a codec writes fields in constructor order, or in
 * the order of their `@PersistedFieldOrder` keys when the properties carry them, reads each into a local in that
 * order, then calls the primary constructor once, so an `init` block runs as it does for any construction and sees
 * every argument. Compiled by both pinned compilers.
 *
 * Without the annotation, a constructor reorder reorders the bytes. With it, a reorder that keeps the keys keeps the
 * bytes, and still changes a data class's generated `hashCode()`, and with it the key group Flink files a partition
 * key under, which is why the snapshot records constructor order beside the persisted order.
 */
class PersistedOrderTest {
    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `by default a record is written in constructor order, so a constructor reorder reorders its bytes`(compiler: KotlinCompiler) {
        val declared = load(Declarations.KEY_AB, compiler)
        val reordered = load(Declarations.KEY_BA, compiler)
        val before = Declarations.samples(declared).single()
        val after = Declarations.samples(reordered).single()
        assertEquals("Key(a=1, b=2)", before.toString())
        assertEquals("Key(b=2, a=1)", after.toString())

        assertEquals("00 00 00 01 00 00 00 02", codec(declared).write(before).hex())
        assertEquals("00 00 00 02 00 00 00 01", codec(reordered).write(after).hex())
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `with PersistedFieldOrder a constructor reorder keeps the bytes, and moves a partition key's key group`(compiler: KotlinCompiler) {
        // Key(@PersistedFieldOrder(10) a, @PersistedFieldOrder(20) b), reordered to Key(@PersistedFieldOrder(7) b,
        // @PersistedFieldOrder(3) a): renumbered, sparse keys that keep the persisted order a, b.
        val declared = load(Declarations.KEY_AB_PINNED, compiler)
        val reordered = load(Declarations.KEY_BA_PINNED, compiler)
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
    fun `decoding calls the current constructor once, with every field read, so the init block sees them all`(compiler: KotlinCompiler) {
        val cases = mapOf(
            Declarations.KEY_AB to listOf("00 00 00 01 00 00 00 02", "00 00 00 03 00 00 00 02"),
            Declarations.KEY_BA to listOf("00 00 00 02 00 00 00 01", "00 00 00 02 00 00 00 03"),
            Declarations.KEY_AB_PINNED to listOf("00 00 00 01 00 00 00 02", "00 00 00 03 00 00 00 02"),
            Declarations.KEY_BA_PINNED to listOf("00 00 00 01 00 00 00 02", "00 00 00 03 00 00 00 02"),
        )
        for ((source, inputs) in cases) {
            val codec = codec(load(source, compiler))
            val (valid, invalid) = inputs
            assertEquals("1 2", codec.deserialize(DataInputDeserializer(bytes(valid))).let { "${it.field("a")} ${it.field("b")}" })
            // a = 3 and b = 2 fail the init block's check across both fields, as constructing Key directly would.
            val e = assertThrows<IllegalArgumentException> { codec.deserialize(DataInputDeserializer(bytes(invalid))) }
            assertEquals("a must not exceed b", e.message)
        }
    }

    /** Each declaration is compiled once per compiler, for all the tests. */
    private fun load(source: String, compiler: KotlinCompiler): ClassLoader =
        loaded.getOrPut(source to compiler) { Declarations.load(source, compiler) }

    @Suppress("UNCHECKED_CAST")
    private fun codec(loader: ClassLoader): GeneratedCodec<Any> =
        (TypeRegistry.load(loader).resolve(kotlin.jvm.internal.Reflection.typeOf(loader.loadClass("com.example.Key"))) as Resolution.Codec)
            .codec as GeneratedCodec<Any>

    private fun Any.field(name: String): Any = javaClass.getMethod("get" + name.replaceFirstChar(Char::uppercaseChar)).invoke(this)

    private companion object {
        val loaded = HashMap<Pair<String, KotlinCompiler>, ClassLoader>()
    }
}
