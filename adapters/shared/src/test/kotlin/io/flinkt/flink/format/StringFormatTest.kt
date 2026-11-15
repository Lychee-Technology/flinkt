package io.flinkt.flink.format

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.UTFDataFormatException
import java.lang.management.ManagementFactory
import kotlin.random.Random
import org.apache.flink.core.memory.DataInputDeserializer
import org.apache.flink.types.StringValue
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * The string format of record format version 1 (docs/persisted-format.md, "Strings"): Flink's `StringValue`
 * char-varint format, specified as Flinkt's own. On this adapter's line, the prototype writes the bytes Flink's
 * `StringValue.writeString` writes, on a `DataOutputSerializer` and on a paged view, and each reads what the other
 * wrote. Flinkt's reader is stricter, and doesn't allocate for a length it hasn't read.
 */
class StringFormatTest {
    @Test
    fun `examples`() {
        val examples = linkedMapOf(
            "" to "01",
            "A" to "02 41",
            "Ada" to "04 41 64 61",
            "é" to "02 e9 01", // U+00E9: two bytes
            "€" to "02 ac 41", // U+20AC: two bytes
            "中" to "02 ad 9c 01", // U+4E2D: three bytes
            "😀" to "03 bd b0 03 80 bc 03", // U+1F600, the surrogate pair D83D DE00: three bytes each
            "\uD800" to "02 80 b0 03", // an unpaired high surrogate
            "\uDC00a" to "03 80 b8 03 61", // an unpaired low surrogate, then a
            "a".repeat(127) to "80 01" + " 61".repeat(127), // the length plus one, 128, takes two bytes
        )
        for ((string, expected) in examples) {
            assertEquals(normalized(expected), written { RecordFormat.writeString(it, string) }.hex(), string)
            assertEquals(string, RecordFormat.readString(DataInputDeserializer(bytes(expected))))
        }
    }

    @Test
    fun `Flinkt's bytes are StringValue's bytes, and each reads what the other wrote`() {
        for (string in corpus()) {
            val ours = written { RecordFormat.writeString(it, string) }
            val flinks = written { StringValue.writeString(string, it) }
            assertEquals(hex(flinks.serializer), hex(flinks.paged), "Flink's own bytes for ${describe(string)}")
            assertArrayEquals(flinks.serializer, ours.bytes, describe(string))

            assertEquals(string, StringValue.readString(DataInputDeserializer(ours.bytes)), describe(string))
            val input = DataInputDeserializer(flinks.serializer)
            assertEquals(string, RecordFormat.readString(input), describe(string))
            assertEquals(0, input.available())
        }
    }

    @Test
    fun `every String round-trips, unpaired surrogates included, and distinct strings have distinct bytes`() {
        val strings = corpus()
        val seen = HashMap<String, String>()
        for (string in strings) {
            val bytes = written { RecordFormat.writeString(it, string) }.bytes
            assertEquals(string, RecordFormat.readString(DataInputDeserializer(bytes)))
            val previous = seen.put(hex(bytes), string)
            assertTrue(previous == null || previous == string) { "${describe(string)} and ${describe(previous!!)}" }
        }
    }

    @Test
    fun `the formats it replaces lose surrogates or limit the length`() {
        // UTF-8 replaces an unpaired surrogate with '?', so two different keys would have the same bytes.
        assertArrayEquals("?".toByteArray(Charsets.UTF_8), "\uD800".toByteArray(Charsets.UTF_8))
        // writeUTF refuses an encoding over 65,535 bytes.
        val long = "a".repeat(65_536)
        assertThrows<UTFDataFormatException> { DataOutputStream(ByteArrayOutputStream()).writeUTF(long) }
        assertEquals(long, RecordFormat.readString(DataInputDeserializer(written { RecordFormat.writeString(it, long) }.bytes)))
    }

    @Test
    fun `strict reads reject what no string encodes`() {
        fun fails(hex: String, message: String) {
            val e = assertThrows<MalformedDataException>(hex) { RecordFormat.readString(DataInputDeserializer(bytes(hex))) }
            assertTrue(message in e.message.orEmpty(), "${e.message}")
            val skip = assertThrows<MalformedDataException>(hex) { RecordFormat.skipString(DataInputDeserializer(bytes(hex))) }
            assertEquals(e.message, skip.message)
        }
        fails("00", "never 00")
        fails("81 00", "a string length has a varint with a redundant last byte 00")
        fails("ff ff ff ff ff 01", "a string length has a varint longer than 5 bytes")
        fails("ff ff ff ff 0f", "a string length is 4294967295, over 2147483647")
        fails("02 c1 80 00", "a char has a varint with a redundant last byte 00")
        fails("02 ff ff 7f", "a char is 2097151, over 65535")
        fails("02 ff ff ff 01", "a char has a varint longer than 3 bytes")
        assertThrows<EOFException> { RecordFormat.readString(DataInputDeserializer(bytes("04 41 64"))) }
    }

    @Test
    fun `a corrupt length fails at the end of the input, without allocating for the length it claims`() {
        // The length plus one is 67,108,865: a string of 67,108,864 chars, the budget, but only three chars follow.
        // Flink's StringValue.readString allocates an array of the claimed length before it reads a char. A longer
        // claim fails at the length (ResourceLimitsTest).
        assertEquals(1 shl 26, RecordFormat.MAX_STRING_LENGTH)
        val claim = bytes("81 80 80 20 41 41 41")
        val threads = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
        val before = threads.getThreadAllocatedBytes(Thread.currentThread().id)
        assertThrows<EOFException> { RecordFormat.readString(DataInputDeserializer(claim)) }
        val allocated = threads.getThreadAllocatedBytes(Thread.currentThread().id) - before
        assertTrue(allocated < 1_000_000, "reading allocated $allocated bytes")
    }

    private fun corpus(): List<String> {
        val random = Random(5)
        val fixed = listOf(
            "", "a", "Ada", "\u0000", "\u007f", "\u0080", "߿", "ࠀ", "㿿", "䀀", "￿", "�",
            "\uD800", "\uDBFF", "\uDC00", "\uDFFF", "\uDC00\uD800", "𐀀", "😀", "a\uD800b", "é", "é",
        )
        // Lengths whose length-plus-one varint changes size, and a string longer than writeUTF allows.
        val lengths = listOf(126, 127, 128, 16_382, 16_383, 16_384, 70_000).map { "x".repeat(it) }
        // Strings of any UTF-16 code units, surrogates included, in any order.
        val randomStrings = List(500) { String(CharArray(random.nextInt(0, 40)) { random.nextInt(0, 0x10000).toChar() }) }
        return fixed + lengths + randomStrings
    }

    private fun describe(string: String): String =
        if (string.length > 20) "a string of ${string.length} chars" else string.map { "U+%04X".format(it.code) }.toString()
}
