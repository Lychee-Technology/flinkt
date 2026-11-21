package io.flinkt.codec

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

/**
 * Bytes that no value encodes: a corrupt, truncated, or misread record or snapshot.
 */
public class MalformedDataException(message: String) : IOException(message)

/**
 * Record format version 1 primitives for built-in scalars and strings (docs/persisted-format.md, "Records").
 *
 * Generated codecs call these directly through [DataOutput] and [DataInput] without mentioning any Flink type.
 * Writes use only `writeByte`, `writeShort`, `writeChar`, `writeInt`, and `writeLong`, which produce identical bytes
 * on every Flink output view; `writeFloat`, `writeDouble`, `writeBytes(String)`, and `writeUTF` are never used.
 *
 * Readers are strict: byte sequences that no value encodes fail with [MalformedDataException].
 */
public object RecordFormat {
    /** The record format version written and read by these primitives. */
    public const val VERSION: Int = 1

    /**
     * Maximum persisted string length in UTF-16 code units (2^26). Both writer and reader enforce it so a release
     * never writes a string it would refuse to read.
     */
    public const val MAX_STRING_LENGTH: Int = 1 shl 26

    /** Initial char buffer cap when decoding a string; grows by doubling as chars are actually read. */
    public const val STRING_CHUNK: Int = 8192

    private const val CANONICAL_FLOAT_NAN: Int = 0x7fc00000
    private const val CANONICAL_DOUBLE_NAN: Long = 0x7ff8000000000000L
    private const val MAX_CHAR_BYTES: Int = 3
    private const val MAX_LENGTH_BYTES: Int = 5

    @JvmStatic
    public fun writeBoolean(output: DataOutput, value: Boolean) {
        output.writeByte(if (value) 1 else 0)
    }

    @JvmStatic
    public fun readBoolean(input: DataInput): Boolean = when (val byte = input.readUnsignedByte()) {
        0 -> false
        1 -> true
        else -> throw MalformedDataException("a Boolean is 00 or 01, not ${hexByte(byte)}")
    }

    @JvmStatic
    public fun writeByte(output: DataOutput, value: Byte) {
        output.writeByte(value.toInt())
    }

    @JvmStatic
    public fun readByte(input: DataInput): Byte = input.readByte()

    @JvmStatic
    public fun writeShort(output: DataOutput, value: Short) {
        output.writeShort(value.toInt())
    }

    @JvmStatic
    public fun readShort(input: DataInput): Short = input.readShort()

    @JvmStatic
    public fun writeChar(output: DataOutput, value: Char) {
        output.writeChar(value.code)
    }

    @JvmStatic
    public fun readChar(input: DataInput): Char = input.readChar()

    @JvmStatic
    public fun writeInt(output: DataOutput, value: Int) {
        output.writeInt(value)
    }

    @JvmStatic
    public fun readInt(input: DataInput): Int = input.readInt()

    @JvmStatic
    public fun writeLong(output: DataOutput, value: Long) {
        output.writeLong(value)
    }

    @JvmStatic
    public fun readLong(input: DataInput): Long = input.readLong()

    /** `floatToIntBits` through `writeInt`: every NaN is written as `7f c0 00 00` on every output view. */
    @JvmStatic
    public fun writeFloat(output: DataOutput, value: Float) {
        output.writeInt(java.lang.Float.floatToIntBits(value))
    }

    @JvmStatic
    public fun readFloat(input: DataInput): Float {
        val bits = input.readInt()
        if (bits and 0x7fffffff > 0x7f800000 && bits != CANONICAL_FLOAT_NAN) {
            throw MalformedDataException("a Float NaN is written as 7f c0 00 00, not ${hexInt(bits)}")
        }
        return java.lang.Float.intBitsToFloat(bits)
    }

    /** `doubleToLongBits` through `writeLong`: every NaN is written as `7f f8 00 00 00 00 00 00` on every output view. */
    @JvmStatic
    public fun writeDouble(output: DataOutput, value: Double) {
        output.writeLong(java.lang.Double.doubleToLongBits(value))
    }

    @JvmStatic
    public fun readDouble(input: DataInput): Double {
        val bits = input.readLong()
        if (bits and Long.MAX_VALUE > 0x7ff0000000000000L && bits != CANONICAL_DOUBLE_NAN) {
            throw MalformedDataException("a Double NaN is written as 7f f8 00 00 00 00 00 00, not ${hexLong(bits)}")
        }
        return java.lang.Double.longBitsToDouble(bits)
    }

    /**
     * Writes [value] in Flinkt's char-varint string format (matching Flink's `StringValue.writeString`):
     * length in UTF-16 code units plus one as an unsigned base-128 varint (7 bits per byte, low group first),
     * followed by each UTF-16 code unit as an unsigned varint. Lossless for every `String`, unpaired surrogates
     * included. Fails before writing if `value.length > MAX_STRING_LENGTH`.
     */
    @JvmStatic
    public fun writeString(output: DataOutput, value: String) {
        val length = value.length
        require(length <= MAX_STRING_LENGTH) {
            "A String of $length chars is over the budget of $MAX_STRING_LENGTH chars, so it can't be persisted"
        }
        writeVarint(output, length + 1)
        for (i in 0 until length) {
            writeVarint(output, value[i].code)
        }
    }

    /**
     * Reads a string written by [writeString]. Rejects `00` length, non-minimal varints, lengths over [MAX_STRING_LENGTH],
     * and allocates at most [STRING_CHUNK] chars up front before doubling as chars are read.
     */
    @JvmStatic
    public fun readString(input: DataInput): String {
        val lengthPlusOne = readVarint(input, MAX_LENGTH_BYTES, Int.MAX_VALUE.toLong(), "a string length").toInt()
        if (lengthPlusOne == 0) {
            throw MalformedDataException(
                "a string length is written plus one, so it's never 00. Flink's StringValue writes 00 for null, " +
                    "and Flinkt writes null in a null bitmap or a null marker",
            )
        }
        val length = lengthPlusOne - 1
        if (length > MAX_STRING_LENGTH) {
            throw MalformedDataException("a string is $length chars long, over the budget of $MAX_STRING_LENGTH")
        }
        var chars = CharArray(minOf(length, STRING_CHUNK))
        for (i in 0 until length) {
            if (i == chars.size) chars = chars.copyOf(minOf(length, chars.size * 2))
            chars[i] = readVarint(input, MAX_CHAR_BYTES, Char.MAX_VALUE.code.toLong(), "a char").toInt().toChar()
        }
        return String(chars)
    }

    private fun writeVarint(output: DataOutput, value: Int) {
        var rest = value
        while (rest >= 0x80) {
            output.writeByte(rest and 0x7f or 0x80)
            rest = rest ushr 7
        }
        output.writeByte(rest)
    }

    private fun readVarint(input: DataInput, maxBytes: Int, maxValue: Long, what: String): Long {
        var value = 0L
        var shift = 0
        var count = 0
        while (true) {
            val byte = input.readUnsignedByte()
            count++
            value = value or ((byte and 0x7f).toLong() shl shift)
            if (byte < 0x80) {
                if (count > 1 && byte == 0) {
                    throw MalformedDataException("$what has a varint with a redundant last byte 00")
                }
                break
            }
            if (count == maxBytes) {
                throw MalformedDataException("$what has a varint longer than $maxBytes bytes")
            }
            shift += 7
        }
        if (value > maxValue) {
            throw MalformedDataException("$what is $value, over $maxValue")
        }
        return value
    }

    private const val HEX_DIGITS: String = "0123456789abcdef"

    private fun hexByte(byte: Int): String =
        charArrayOf(HEX_DIGITS[(byte ushr 4) and 0xf], HEX_DIGITS[byte and 0xf]).concatToString()

    private fun hexInt(bits: Int): String = buildString(11) {
        for (shift in 24 downTo 0 step 8) {
            if (shift != 24) append(' ')
            append(hexByte((bits ushr shift) and 0xff))
        }
    }

    private fun hexLong(bits: Long): String = buildString(23) {
        for (shift in 56 downTo 0 step 8) {
            if (shift != 56) append(' ')
            append(hexByte(((bits ushr shift) and 0xffL).toInt()))
        }
    }
}
