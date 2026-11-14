package io.flinkt.flink.format

import io.flinkt.codec.GeneratedCodec
import java.io.EOFException
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.BooleanSerializer
import org.apache.flink.api.common.typeutils.base.ByteSerializer
import org.apache.flink.api.common.typeutils.base.CharSerializer
import org.apache.flink.api.common.typeutils.base.DoubleSerializer
import org.apache.flink.api.common.typeutils.base.FloatSerializer
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.typeutils.base.ShortSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.core.memory.DataInputDeserializer
import org.apache.flink.core.memory.DataOutputView
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Record format version 1 (docs/persisted-format.md, "Records"), as #5's evidence, on this adapter's Flink line: the
 * byte-level examples, written through a `DataOutputSerializer` and through a paged view with the same bytes, and the
 * strict reads. #12 and #15 turn the examples into golden fixtures of the generated codecs.
 */
class RecordFormatTest {
    /** A Float NaN and a Double NaN whose payloads aren't the canonical ones. */
    private val floatNaN = java.lang.Float.intBitsToFloat(0x7f800001)
    private val doubleNaN = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)

    @Test
    fun `User(42, Ada) is its id then its name, with no header`() {
        assertGolden(UserCodec, User(42, "Ada"), "00 00 00 00 00 00 00 2a | 04 41 64 61")
    }

    @Test
    fun `the nullable variant starts with its bitmap, and writes nothing for a null field`() {
        assertGolden(
            AccountCodec,
            Account(id = 7, name = "Ada", email = null, balance = doubleNaN, rating = floatNaN),
            """
            02                          bitmap: balance present, email null, name present
            7f f8 00 00 00 00 00 00     balance: every NaN is written canonical
            00 00 00 00 00 00 00 07     id
            04 41 64 61                 name
            7f c0 00 00                 rating
            """.let(::annotatedHex),
        )
        assertGolden(
            AccountCodec,
            Account(id = 7, name = null, email = null, balance = null, rating = -0.0f),
            "07 | 00 00 00 00 00 00 00 07 | 80 00 00 00",
        )
    }

    @Test
    fun `a constructor reorder changes no bytes, because fields are written by name`() {
        // Account's constructor order is id, name, email, balance, rating. Its bytes follow the names.
        val value = Account(id = 1, name = "n", email = "e", balance = 2.0, rating = 3.0f)
        assertEquals(
            normalized("00 | 40 00 00 00 00 00 00 00 | 02 65 | 00 00 00 00 00 00 00 01 | 02 6e | 40 40 00 00"),
            AccountCodec.write(value).hex(),
        )
    }

    @Test
    fun `nested records are written inline, and a nullable one is a bitmap bit`() {
        assertGolden(
            SessionCodec,
            Session(user = User(42, "Ada"), note = null, previous = null),
            "03 | 00 00 00 00 00 00 00 2a 04 41 64 61",
        )
        assertGolden(
            SessionCodec,
            Session(user = User(42, "Ada"), note = "hi", previous = User(41, "Al")),
            "00 | 03 68 69 | 00 00 00 00 00 00 00 29 03 41 6c | 00 00 00 00 00 00 00 2a 04 41 64 61",
        )
    }

    @Test
    fun `nine nullable fields take two bitmap bytes, least significant bit first`() {
        val allNull = Wide(null, null, null, null, null, null, null, null, null)
        assertGolden(WideCodec, allNull, "ff 01")
        assertGolden(WideCodec, allNull.copy(a = true), "fe 01 | 01")
        assertGolden(WideCodec, allNull.copy(i = false), "ff 00 | 00")
        assertGolden(WideCodec, Wide(true, true, true, true, true, true, true, true, true), "00 00 | 01 01 01 01 01 01 01 01 01")
    }

    @Test
    fun `every scalar kind, at boundary values`() {
        assertGolden(
            ScalarsCodec,
            Scalars(
                z = true, b = -1, s = Short.MIN_VALUE, i = Int.MIN_VALUE, l = Long.MAX_VALUE, f = -0.0f,
                d = Double.MIN_VALUE, c = '\uD800', t = "\uDE00",
            ),
            """
            ff                          b
            d8 00                       c: an unpaired surrogate
            00 00 00 00 00 00 00 01     d
            80 00 00 00                 f: negative zero
            80 00 00 00                 i
            7f ff ff ff ff ff ff ff     l
            80 00                       s
            02 80 bc 03                 t: an unpaired low surrogate
            01                          z
            """.let(::annotatedHex),
        )
        assertGolden(
            ScalarsCodec,
            Scalars(
                z = false, b = Byte.MAX_VALUE, s = Short.MAX_VALUE, i = Int.MAX_VALUE, l = Long.MIN_VALUE,
                f = Float.NEGATIVE_INFINITY, d = doubleNaN, c = Char.MAX_VALUE, t = "",
            ),
            """
            7f | ff ff | 7f f8 00 00 00 00 00 00 | ff 80 00 00 | 7f ff ff ff | 80 00 00 00 00 00 00 00 | 7f ff | 01 | 00
            """.let(::annotatedHex),
        )
    }

    @Test
    fun `a top-level nullable type is one marker byte, then the non-null bytes`() {
        val codec = NullableCodec(UserCodec)
        assertGolden(codec, null, "00")
        assertGolden(codec, User(42, "Ada"), "01 | 00 00 00 00 00 00 00 2a 04 41 64 61")
    }

    @Test
    fun `each scalar's bytes are Flink's built-in serializer's bytes on a DataOutputSerializer`() {
        fun <T> check(serializer: TypeSerializer<T>, values: List<T>, flinkt: (DataOutputView, T) -> Unit) {
            for (value in values) {
                val ours = written { flinkt(it, value) }
                val flinks = written { serializer.serialize(value, it) }
                assertEquals(hex(flinks.serializer), ours.hex(), "$value")
            }
        }
        check(BooleanSerializer.INSTANCE, listOf(false, true)) { out, v -> RecordFormat.writeBoolean(out, v) }
        check(ByteSerializer.INSTANCE, listOf<Byte>(0, -1, Byte.MIN_VALUE, Byte.MAX_VALUE)) { out, v -> RecordFormat.writeByte(out, v) }
        check(ShortSerializer.INSTANCE, listOf<Short>(0, -1, Short.MIN_VALUE, Short.MAX_VALUE)) { out, v -> RecordFormat.writeShort(out, v) }
        check(IntSerializer.INSTANCE, listOf(0, -1, Int.MIN_VALUE, Int.MAX_VALUE)) { out, v -> RecordFormat.writeInt(out, v) }
        check(LongSerializer.INSTANCE, listOf(0, -1, Long.MIN_VALUE, Long.MAX_VALUE)) { out, v -> RecordFormat.writeLong(out, v) }
        check(CharSerializer.INSTANCE, listOf('a', '\u0000', '\uD800', '￿')) { out, v -> RecordFormat.writeChar(out, v) }
        check(StringSerializer.INSTANCE, listOf("", "Ada", "\uD800", "😀")) { out, v -> RecordFormat.writeString(out, v) }
        check(FloatSerializer.INSTANCE, listOf(0.0f, -0.0f, Float.MIN_VALUE, Float.NaN, floatNaN, Float.NEGATIVE_INFINITY)) { out, v ->
            RecordFormat.writeFloat(out, v)
        }
        check(DoubleSerializer.INSTANCE, listOf(0.0, -0.0, Double.MIN_VALUE, Double.NaN, doubleNaN, Double.POSITIVE_INFINITY)) { out, v ->
            RecordFormat.writeDouble(out, v)
        }
    }

    @Test
    fun `on a paged view, Flink's Float and Double serializers keep a NaN's payload, and Flinkt's format doesn't`() {
        val flinkFloat = written { FloatSerializer.INSTANCE.serialize(floatNaN, it) }
        assertEquals("7f c0 00 00", hex(flinkFloat.serializer))
        assertEquals("7f 80 00 01", hex(flinkFloat.paged))
        val flinkDouble = written { DoubleSerializer.INSTANCE.serialize(doubleNaN, it) }
        assertEquals("7f f8 00 00 00 00 00 00", hex(flinkDouble.serializer))
        assertEquals("7f f0 00 00 00 00 00 01", hex(flinkDouble.paged))

        assertEquals("7f c0 00 00", written { RecordFormat.writeFloat(it, floatNaN) }.hex())
        assertEquals("7f f8 00 00 00 00 00 00", written { RecordFormat.writeDouble(it, doubleNaN) }.hex())
    }

    @Test
    fun `equal key components have equal bytes, and unequal ones different bytes`() {
        // The key-eligible built-ins, compared as a data class compares them: by value, and a Float or Double by
        // its floatToIntBits or doubleToLongBits. A record's bytes concatenate self-delimiting components in a fixed
        // order, so the same holds for a record of eligible components.
        val strings = listOf("", "a", "A", "ab", "a\u0000", "\u0000", "?", "\uD800", "\uDC00", "𐀀", "😀", "�", "é", "é")
        val floats = listOf(0.0f, -0.0f, 1.0f, Float.NaN, floatNaN, java.lang.Float.intBitsToFloat(-0x400000), Float.MIN_VALUE)
        val doubles = listOf(0.0, -0.0, 1.0, Double.NaN, doubleNaN, java.lang.Double.longBitsToDouble(-0x8000000000000L))
        assertKeyBytes(strings.map(::StringKey), StringKey.CODEC)
        assertKeyBytes(floats.map(::FloatKey), FloatKey.CODEC)
        assertKeyBytes(doubles.map(::DoubleKey), DoubleKey.CODEC)
        assertKeyBytes(
            strings.flatMap { s -> listOf(0L, 1L, -1L).map { CompositeKey(s, it) } } +
                listOf(CompositeKey("a", 0x0102030405060708L)),
            CompositeKey.CODEC,
        )
        // A record's equality treats every NaN as one value and -0.0 as another value than 0.0, as its bytes do.
        assertEquals(DoubleKey(Double.NaN), DoubleKey(doubleNaN))
        assertNotEquals(DoubleKey(0.0), DoubleKey(-0.0))
    }

    private fun <K> assertKeyBytes(keys: List<K>, codec: GeneratedCodec<K>) {
        for (a in keys) {
            for (b in keys) {
                val sameBytes = codec.write(a).bytes.contentEquals(codec.write(b).bytes)
                assertEquals(a == b, sameBytes, "$a and $b")
            }
        }
    }

    @Test
    fun `strict reads reject bytes that no value encodes`() {
        fun fails(codec: GeneratedCodec<*>, hex: String, message: String) {
            val e = assertThrows<MalformedDataException>(hex) { codec.deserialize(DataInputDeserializer(bytes(hex))) }
            assertTrue(message in e.message.orEmpty(), "${e.message}")
        }
        val scalars = "ff | d8 00 | 00 00 00 00 00 00 00 01 | 80 00 00 00 | 80 00 00 00 | 7f ff ff ff ff ff ff ff | 80 00 | 01 | "
        fails(ScalarsCodec, scalars + "02", "a Boolean is 00 or 01, not 02")
        fails(ScalarsCodec, "ff d8 00 7f f0 00 00 00 00 00 01", "a Double NaN is written as 7f f8 00 00 00 00 00 00, not 7f f0 00 00 00 00 00 01")
        fails(WideCodec, "ff 03", "has bit 9 set")
        fails(AccountCodec, "08", "Account's null bitmap has 3 bits, not 08")
        fails(NullableCodec(UserCodec), "02", "a null marker is 00 or 01, not 02")
        assertThrows<EOFException> { UserCodec.deserialize(DataInputDeserializer(bytes("00 00 00 00 00 00 00 2a 04 41 64"))) }
    }

    private fun <T> assertGolden(codec: GeneratedCodec<T>, value: T, expected: String) {
        val written = codec.write(value)
        assertEquals(normalized(expected), written.hex(), "$value")
        val input = DataInputDeserializer(written.bytes)
        val decoded = codec.deserialize(input)
        assertEquals(0, input.available(), "$value left bytes unread")
        // A NaN with a payload decodes as the canonical NaN, which is equal in a data class.
        assertEquals(value, decoded)
        assertEquals(written.hex(), codec.write(decoded).hex())
    }
}

data class StringKey(val value: String) {
    companion object {
        val CODEC = key<StringKey>({ RecordFormat.writeString(it, value) }) { StringKey(RecordFormat.readString(it)) }
    }
}

data class FloatKey(val value: Float) {
    companion object {
        val CODEC = key<FloatKey>({ RecordFormat.writeFloat(it, value) }) { FloatKey(RecordFormat.readFloat(it)) }
    }
}

data class DoubleKey(val value: Double) {
    companion object {
        val CODEC = key<DoubleKey>({ RecordFormat.writeDouble(it, value) }) { DoubleKey(RecordFormat.readDouble(it)) }
    }
}

data class CompositeKey(val name: String, val id: Long) {
    companion object {
        val CODEC = key<CompositeKey>({
            RecordFormat.writeLong(it, id)
            RecordFormat.writeString(it, name)
        }) {
            val id = RecordFormat.readLong(it)
            CompositeKey(RecordFormat.readString(it), id)
        }
    }
}

private fun <K> key(write: K.(java.io.DataOutput) -> Unit, read: (java.io.DataInput) -> K): GeneratedCodec<K> =
    object : GeneratedCodec<K> {
        override fun serialize(value: K, output: java.io.DataOutput) = value.write(output)

        override fun deserialize(input: java.io.DataInput): K = read(input)

        override fun copy(value: K): K = value
    }
