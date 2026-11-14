package io.flinkt.codec

import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
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
 * `deserialize` and `copy` rebuild a record by calling its primary constructor with the constructor properties they
 * read, so an `init` block runs on every rebuild (docs/architecture.md, "Rebuilding through the constructor").
 *
 * flinkt-ksp doesn't generate codecs yet (#12), so the records' codecs here are stand-ins that rebuild the way generated
 * ones do: read each constructor property, then call the primary constructor, with a nested record's codec fixed when
 * the outer one is created. What an `init` block does at construction is code the compiler emits into the record's
 * class, so the records are compiled by both pinned compilers and loaded into this test. The cases whose `init` block
 * changes a property show that these assertions catch a rebuild that changes a value.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConstructorRebuildTest {
    private val loaders = ConcurrentHashMap<KotlinCompiler, ClassLoader>()

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
            val written = sample.write(value)

            val decoded = sample.read(written)
            assertEquals(value, decoded, "deserialize(serialize(v)) of $name")
            assertArrayEquals(written, sample.write(decoded), "bytes of deserialize(serialize(v)) of $name")

            val copied = sample.codec.copy(value)
            assertEquals(value, copied, "copy(v) of $name")
            assertNotSame(value, copied, "copy(v) of $name")
            assertArrayEquals(written, sample.write(copied), "bytes of copy(v) of $name")
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
            // Writing doesn't run the constructor, so nothing fails until the value is rebuilt.
            val written = sample.write(sample.value)

            val decoding = assertThrows<IllegalArgumentException>(name) { sample.read(written) }
            assertEquals(CHECK_MESSAGE, decoding.message, name)
            val copying = assertThrows<IllegalArgumentException>(name) { sample.codec.copy(sample.value) }
            assertEquals(CHECK_MESSAGE, copying.message, name)
        }
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `an init block that changes a constructor property changes the value on every rebuild`(compiler: KotlinCompiler) {
        val samples = samples(compiler)

        // Each line: the value, what deserialize(serialize(v)) gives, and what copying that gives.
        samples.assertRebuilds("Counter(0)", "Counter(count=1)", "Counter(count=2)", "Counter(count=3)")
        samples.assertRebuilds(
            "CounterHolder(Counter(0))",
            "CounterHolder(counter=Counter(count=1))",
            "CounterHolder(counter=Counter(count=2))",
            "CounterHolder(counter=Counter(count=3))",
        )
        // Outer has only vals and Inner has no init block, and Outer's init block still changes Inner's var.
        samples.assertRebuilds(
            "Outer(Inner(0))",
            "Outer(inner=Inner(count=1))",
            "Outer(inner=Inner(count=2))",
            "Outer(inner=Inner(count=3))",
        )
        // Normalizing is idempotent on what the constructor built, but a var can be assigned anything.
        samples.assertRebuilds("Email(A)", "Email(address=a)", "Email(address=a)", "Email(address=a)")
        samples.assertRebuilds("Email(A), address = B", "Email(address=B)", "Email(address=b)", "Email(address=b)")
    }

    private fun Map<String, Sample>.assertRebuilds(name: String, shown: String, decodedShown: String, copiedShown: String) {
        val sample = getValue(name)
        assertEquals(shown, sample.value.toString(), name)
        val written = sample.write(sample.value)
        val decoded = sample.read(written)
        assertEquals(decodedShown, decoded.toString(), "deserialize(serialize(v)) of $name")
        assertEquals(copiedShown, sample.codec.copy(decoded).toString(), "copy(deserialize(serialize(v))) of $name")
        // The bytes tell the two apart wherever toString() does.
        assertEquals(shown == decodedShown, written.contentEquals(sample.write(decoded)), "bytes of $name")
        if (shown != decodedShown) assertFalse(sample.value == decoded, name)
    }

    private fun samples(compiler: KotlinCompiler): Map<String, Sample> {
        val loader = loaders.computeIfAbsent(compiler) { KotlinCompilation(it).compile(RECORDS).classLoader() }

        @Suppress("UNCHECKED_CAST")
        val samples = loader.loadClass("com.example.Samples").getDeclaredConstructor().newInstance()
            as () -> Map<String, Pair<GeneratedCodec<*>, Any>>
        @Suppress("UNCHECKED_CAST")
        return samples().mapValues { (_, sample) -> Sample(sample.first as GeneratedCodec<Any>, sample.second) }
    }

    /** One value built in the compiled code, with its type's codec. */
    private class Sample(val codec: GeneratedCodec<Any>, val value: Any) {
        fun write(value: Any): ByteArray =
            ByteArrayOutputStream().also { codec.serialize(value, DataOutputStream(it)) }.toByteArray()

        fun read(bytes: ByteArray): Any = codec.deserialize(DataInputStream(ByteArrayInputStream(bytes)))
    }

    private companion object {
        const val CHECK_MESSAGE = "count must not be negative"

        val RECORDS = """
            package com.example

            import io.flinkt.FlinkType
            import io.flinkt.codec.GeneratedCodec
            import java.io.DataInput
            import java.io.DataOutput

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

            // Scalars are written with DataOutput's own calls. The real encodings are #5's, and don't matter here.
            object IntCodec : GeneratedCodec<Int> {
                override fun serialize(value: Int, output: DataOutput) = output.writeInt(value)
                override fun deserialize(input: DataInput): Int = input.readInt()
                override fun copy(value: Int): Int = value
            }

            object StringCodec : GeneratedCodec<String> {
                override fun serialize(value: String, output: DataOutput) = output.writeUTF(value)
                override fun deserialize(input: DataInput): String = input.readUTF()
                override fun copy(value: String): String = value
            }

            /**
             * The codec of a record with one constructor property. It reads the property, and deserialize and copy
             * call the primary constructor with what they read, as a generated codec does.
             */
            class RecordCodec<R, F>(
                private val property: GeneratedCodec<F>,
                private val read: (R) -> F,
                private val construct: (F) -> R,
            ) : GeneratedCodec<R> {
                override fun serialize(value: R, output: DataOutput) = property.serialize(read(value), output)
                override fun deserialize(input: DataInput): R = construct(property.deserialize(input))
                override fun copy(value: R): R = construct(property.copy(read(value)))
            }

            val checkedCodec = RecordCodec(IntCodec, Checked::count, ::Checked)
            val holderCodec = RecordCodec(checkedCodec, Holder::checked, ::Holder)
            val counterCodec = RecordCodec(IntCodec, Counter::count, ::Counter)
            val counterHolderCodec = RecordCodec(counterCodec, CounterHolder::counter, ::CounterHolder)
            val outerCodec = RecordCodec(RecordCodec(IntCodec, Inner::count, ::Inner), Outer::inner, ::Outer)
            val emailCodec = RecordCodec(StringCodec, Email::address, ::Email)

            /** The test can't name these classes, so this builds each value, by name, with its codec. */
            class Samples : () -> Map<String, Pair<GeneratedCodec<*>, Any>> {
                override fun invoke(): Map<String, Pair<GeneratedCodec<*>, Any>> = mapOf(
                    "Checked(2)" to (checkedCodec to Checked(2)),
                    "Checked(1), count = 5" to (checkedCodec to Checked(1).apply { count = 5 }),
                    "Checked(1), count = -1" to (checkedCodec to Checked(1).apply { count = -1 }),
                    "Holder(Checked(2))" to (holderCodec to Holder(Checked(2))),
                    "Holder(Checked(1)), checked.count = 5" to (holderCodec to Holder(Checked(1)).apply { checked.count = 5 }),
                    "Holder(Checked(1)), checked.count = -1" to (holderCodec to Holder(Checked(1)).apply { checked.count = -1 }),
                    "Counter(0)" to (counterCodec to Counter(0)),
                    "CounterHolder(Counter(0))" to (counterHolderCodec to CounterHolder(Counter(0))),
                    "Outer(Inner(0))" to (outerCodec to Outer(Inner(0))),
                    "Email(A)" to (emailCodec to Email("A")),
                    "Email(A), address = B" to (emailCodec to Email("A").apply { address = "B" }),
                )
            }
            """
    }
}
