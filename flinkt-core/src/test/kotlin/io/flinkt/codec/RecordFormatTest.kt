package io.flinkt.codec

import io.flinkt.PersistedFieldOrder
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class RecordFormatTest {
    @Test
    fun `scalars match record format v1 golden bytes`() {
        assertEquals("00", write { RecordFormat.writeBoolean(it, false) })
        assertEquals("01", write { RecordFormat.writeBoolean(it, true) })
        assertEquals("ff", write { RecordFormat.writeByte(it, -1) })
        assertEquals("80 00", write { RecordFormat.writeShort(it, Short.MIN_VALUE) })
        assertEquals("d8 00", write { RecordFormat.writeChar(it, '\uD800') })
        assertEquals("80 00 00 00", write { RecordFormat.writeInt(it, Int.MIN_VALUE) })
        assertEquals("00 00 00 00 00 00 00 2a", write { RecordFormat.writeLong(it, 42L) })
        assertEquals("80 00 00 00", write { RecordFormat.writeFloat(it, -0.0f) })
        assertEquals("7f c0 00 00", write { RecordFormat.writeFloat(it, java.lang.Float.intBitsToFloat(0x7f800001)) })
        assertEquals("80 00 00 00 00 00 00 00", write { RecordFormat.writeDouble(it, -0.0) })
        assertEquals(
            "7f f8 00 00 00 00 00 00",
            write { RecordFormat.writeDouble(it, java.lang.Double.longBitsToDouble(0x7ff0000000000001L)) },
        )
    }

    @Test
    fun `strings match record format v1 char-varint golden bytes and round-trip losslessly`() {
        val cases = listOf(
            "" to "01",
            "Ada" to "04 41 64 61",
            "é" to "02 e9 01",
            "中" to "02 ad 9c 01",
            "😀" to "03 bd b0 03 80 bc 03",
            "\uD800" to "02 80 b0 03",
            "\uDE00" to "02 80 bc 03",
        )
        for ((value, expectedHex) in cases) {
            val bytes = writeBytes { RecordFormat.writeString(it, value) }
            assertEquals(expectedHex, hex(bytes), "string=$value")
            assertEquals(value, read(bytes) { RecordFormat.readString(it) }, "round-trip=$value")
        }
        val length127 = "a".repeat(127)
        val bytes127 = writeBytes { RecordFormat.writeString(it, length127) }
        assertTrue(hex(bytes127).startsWith("80 01 61 61"))
        assertEquals(length127, read(bytes127) { RecordFormat.readString(it) })

        val longString = "x".repeat(RecordFormat.STRING_CHUNK * 3 + 17)
        val longBytes = writeBytes { RecordFormat.writeString(it, longString) }
        assertEquals(longString, read(longBytes) { RecordFormat.readString(it) })
    }

    @Test
    fun `strict reads reject malformed bytes`() {
        assertThrows<MalformedDataException> { read(bytes("02")) { RecordFormat.readBoolean(it) } }
        assertThrows<MalformedDataException> { read(bytes("7f 80 00 01")) { RecordFormat.readFloat(it) } }
        assertThrows<MalformedDataException> { read(bytes("7f f0 00 00 00 00 00 01")) { RecordFormat.readDouble(it) } }
        // String length 00
        assertThrows<MalformedDataException> { read(bytes("00")) { RecordFormat.readString(it) } }
        // Redundant trailing 00 byte in varint
        assertThrows<MalformedDataException> { read(bytes("81 00")) { RecordFormat.readString(it) } }
        // Varint too long for string length (>5 bytes)
        assertThrows<MalformedDataException> { read(bytes("80 80 80 80 80 01")) { RecordFormat.readString(it) } }
        // String length over Int.MAX_VALUE
        assertThrows<MalformedDataException> { read(bytes("80 80 80 80 08")) { RecordFormat.readString(it) } }
        // Char varint over U+FFFF
        assertThrows<MalformedDataException> { read(bytes("02 80 80 04")) { RecordFormat.readString(it) } }
        // String length over budget (MAX_STRING_LENGTH + 1 = 2^26 + 1, written +1 = 2^26 + 2 = 0x04000002 -> 82 80 80 20)
        assertThrows<MalformedDataException> { read(bytes("82 80 80 20")) { RecordFormat.readString(it) } }
        // Truncated string within budget fails with EOF without huge allocation
        assertThrows<EOFException> { read(bytes("81 80 80 20 61")) { RecordFormat.readString(it) } }
    }

    @Test
    fun `PersistedFieldOrder has PROPERTY target and BINARY retention`() {
        val target = PersistedFieldOrder::class.java.getAnnotation(Target::class.java)
        assertEquals(listOf(AnnotationTarget.PROPERTY), target.allowedTargets.toList())
        val retention = PersistedFieldOrder::class.java.getAnnotation(Retention::class.java)
        assertEquals(AnnotationRetention.BINARY, retention.value)
        // BINARY retention means java.lang.annotation.Retention is CLASS (not RUNTIME)
        val javaRetention = PersistedFieldOrder::class.java.getAnnotation(java.lang.annotation.Retention::class.java)
        assertEquals(java.lang.annotation.RetentionPolicy.CLASS, javaRetention.value)
    }

    private fun writeBytes(block: (DataOutputStream) -> Unit): ByteArray =
        ByteArrayOutputStream().also { block(DataOutputStream(it)) }.toByteArray()

    private fun write(block: (DataOutputStream) -> Unit): String = hex(writeBytes(block))

    private fun <T> read(data: ByteArray, block: (DataInputStream) -> T): T {
        val input = DataInputStream(ByteArrayInputStream(data))
        return block(input).also { assertEquals(-1, input.read(), "unread trailing bytes") }
    }

    private fun bytes(hex: String): ByteArray =
        hex.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }.map { it.toInt(16).toByte() }.toByteArray()

    private fun hex(bytes: ByteArray): String = bytes.joinToString(" ") { "%02x".format(it.toInt() and 0xff) }
}
