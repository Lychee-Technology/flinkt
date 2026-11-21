package io.flinkt.ksp

import io.flinkt.codec.GeneratedCodec
import io.flinkt.ksp.KspTestSupport.codecFor
import io.flinkt.ksp.KspTestSupport.compileSingle
import io.flinkt.ksp.KspTestSupport.deserialize
import io.flinkt.ksp.KspTestSupport.serialize
import io.flinkt.registry.TypeRegistry
import io.flinkt.testing.compile.KotlinCompiler
import java.util.concurrent.ConcurrentHashMap
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * Runs the constructor-rebuild cases from `docs/architecture.md` ("Rebuilding through the constructor") and
 * `docs/testing.md` against the real KSP-generated codecs on both pinned compilers, replacing the stand-in codecs
 * of `flinkt-core`'s `ConstructorRebuildTest` once #11 generates real codecs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GeneratedConstructorRebuildTest {
    private val compiled = ConcurrentHashMap<KotlinCompiler, ClassLoader>()

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `an init block that only calls require comes back unchanged from deserialize and copy`(compiler: KotlinCompiler) {
        val samples = samples(compiler)
        val expected = mapOf(
            "Checked(2)" to "Checked(count=2)",
            "Checked(1), count = 5" to "Checked(count=5)",
            "Holder(Checked(2))" to "Holder(checked=Checked(count=2))",
            "Holder(Checked(1)), checked.count = 5" to "Holder(checked=Checked(count=5))",
        )
        for ((name, shown) in expected) {
            val sample = samples.getValue(name)
            val value = sample.value
            assertEquals(shown, value.toString(), name)
            val written = serialize(sample.codec, value)

            val decoded = deserialize(sample.codec, written)
            assertEquals(value, decoded, "deserialize(serialize(v)) of $name")
            assertArrayEquals(written, serialize(sample.codec, decoded), "bytes of deserialize(serialize(v)) of $name")

            val copied = sample.codec.copy(value)
            assertEquals(value, copied, "copy(v) of $name")
            assertNotSame(value, copied, "copy(v) of $name")
            assertArrayEquals(written, serialize(sample.codec, copied), "bytes of copy(v) of $name")
        }
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a value assigned one that fails the check is written, and deserialize and copy throw the init block's exception`(
        compiler: KotlinCompiler,
    ) {
        val samples = samples(compiler)
        for (name in listOf("Checked(1), count = -1", "Holder(Checked(1)), checked.count = -1")) {
            val sample = samples.getValue(name)
            val written = serialize(sample.codec, sample.value)

            val decoding = assertThrows<IllegalArgumentException>(name) { deserialize(sample.codec, written) }
            assertEquals(CHECK_MESSAGE, decoding.message, name)
            val copying = assertThrows<IllegalArgumentException>(name) { sample.codec.copy(sample.value) }
            assertEquals(CHECK_MESSAGE, copying.message, name)
        }
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `an init block that changes a constructor property changes the value on every rebuild`(compiler: KotlinCompiler) {
        val samples = samples(compiler)

        samples.assertRebuilds("Counter(0)", "Counter(count=1)", "Counter(count=2)", "Counter(count=3)")
        samples.assertRebuilds(
            "CounterHolder(Counter(0))",
            "CounterHolder(counter=Counter(count=1))",
            "CounterHolder(counter=Counter(count=2))",
            "CounterHolder(counter=Counter(count=3))",
        )
        samples.assertRebuilds(
            "Outer(Inner(0))",
            "Outer(inner=Inner(count=1))",
            "Outer(inner=Inner(count=2))",
            "Outer(inner=Inner(count=3))",
        )
        samples.assertRebuilds("Email(A)", "Email(address=a)", "Email(address=a)", "Email(address=a)")
        samples.assertRebuilds("Email(A), address = B", "Email(address=B)", "Email(address=b)", "Email(address=b)")
    }

    private fun Map<String, Sample>.assertRebuilds(name: String, shown: String, decodedShown: String, copiedShown: String) {
        val sample = getValue(name)
        assertEquals(shown, sample.value.toString(), name)
        val written = serialize(sample.codec, sample.value)
        val decoded = deserialize(sample.codec, written)
        assertEquals(decodedShown, decoded.toString(), "deserialize(serialize(v)) of $name")
        assertEquals(copiedShown, sample.codec.copy(decoded).toString(), "copy(deserialize(serialize(v))) of $name")
        assertEquals(shown == decodedShown, written.contentEquals(serialize(sample.codec, decoded)), "bytes of $name")
        if (shown != decodedShown) assertFalse(sample.value == decoded, name)
    }

    private fun samples(compiler: KotlinCompiler): Map<String, Sample> {
        val loader = compiled.computeIfAbsent(compiler) {
            compileSingle(RECORDS, compiler = it).assertSuccess().classLoader()
        }
        val registry = TypeRegistry.load(loader)

        @Suppress("UNCHECKED_CAST")
        val values = loader.loadClass("com.example.Samples").getDeclaredConstructor().newInstance()
            as () -> Map<String, Pair<String, Any>>
        return values().mapValues { (_, pair) -> Sample(codecFor(registry, loader, pair.first), pair.second) }
    }

    private class Sample(val codec: GeneratedCodec<Any>, val value: Any)

    private companion object {
        const val CHECK_MESSAGE = "count must not be negative"

        val RECORDS = """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class Checked(var count: Int) {
                init {
                    require(count >= 0) { "$CHECK_MESSAGE" }
                }
            }

            @FlinkType
            data class Holder(val checked: Checked)

            @FlinkType
            data class Counter(var count: Int) {
                init {
                    count++
                }
            }

            @FlinkType
            data class CounterHolder(val counter: Counter)

            @FlinkType
            data class Inner(var count: Int)

            @FlinkType
            data class Outer(val inner: Inner) {
                init {
                    inner.count++
                }
            }

            @FlinkType
            data class Email(var address: String) {
                init {
                    address = address.lowercase()
                }
            }

            class Samples : () -> Map<String, Pair<String, Any>> {
                override fun invoke(): Map<String, Pair<String, Any>> = mapOf(
                    "Checked(2)" to ("com.example.Checked" to Checked(2)),
                    "Checked(1), count = 5" to ("com.example.Checked" to Checked(1).apply { count = 5 }),
                    "Checked(1), count = -1" to ("com.example.Checked" to Checked(1).apply { count = -1 }),
                    "Holder(Checked(2))" to ("com.example.Holder" to Holder(Checked(2))),
                    "Holder(Checked(1)), checked.count = 5" to ("com.example.Holder" to Holder(Checked(1)).apply { checked.count = 5 }),
                    "Holder(Checked(1)), checked.count = -1" to ("com.example.Holder" to Holder(Checked(1)).apply { checked.count = -1 }),
                    "Counter(0)" to ("com.example.Counter" to Counter(0)),
                    "CounterHolder(Counter(0))" to ("com.example.CounterHolder" to CounterHolder(Counter(0))),
                    "Outer(Inner(0))" to ("com.example.Outer" to Outer(Inner(0))),
                    "Email(A)" to ("com.example.Email" to Email("A")),
                    "Email(A), address = B" to ("com.example.Email" to Email("A").apply { address = "B" }),
                )
            }
        """
    }
}
