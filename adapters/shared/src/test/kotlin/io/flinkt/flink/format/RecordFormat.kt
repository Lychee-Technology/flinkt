package io.flinkt.flink.format

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

/**
 * A prototype of record format version 1 (docs/persisted-format.md, "Records"): the bytes of each built-in scalar, of
 * a string, of a record's null bitmap and of the null marker.
 *
 * It's evidence for #5, not production code. #12 and #15 implement the format in `flinkt-core`, which owns the
 * codecs a composed built-in needs, and KSP generates records' codecs that write it (#11). Like those, it's
 * Flink-free: it writes through `DataOutput` and reads through `DataInput`, and the only calls it makes are the ones
 * every Flink output view writes alike (`writeByte`, `writeShort`, `writeChar`, `writeInt`, `writeLong`), which
 * [RecordFormatTest] checks on a `DataOutputSerializer` and on a paged view.
 *
 * Its readers are strict: a byte sequence that no value encodes fails with [MalformedDataException], never decodes to
 * a value that would encode differently.
 */
object RecordFormat {
    /** The record format version this prototype writes and reads. */
    const val VERSION: Int = 1

    /**
     * The budget on a string's length, in UTF-16 chars. A writer refuses a longer string, so a reader never meets one
     * that a release wrote, and fails at the length, before it reads a char.
     */
    const val MAX_STRING_LENGTH: Int = 1 shl 26

    /** The budget on a `List`'s or a `Map`'s size. A writer refuses a larger collection, and a reader fails at the size. */
    const val MAX_COLLECTION_SIZE: Int = 1 shl 26

    /**
     * The budget on a `Map`'s counted keys that share one `hashCode()`: keys that are records or value classes, in a map
     * whose key type is a record, a value class or a sealed type (docs/persisted-format.md, "Keys that share a hash
     * code"). `HashMap` compares a key of such a class with every earlier key of its hash code, so without the budget
     * a map's read would be quadratic in its size. A writer refuses a map over it, and a reader fails at the key that's
     * one too many.
     */
    const val MAX_KEYS_PER_HASH_CODE: Int = 1 shl 10

    /** A string of up to this many chars is read into one array of its length. A longer one starts there and grows. */
    const val STRING_CHUNK: Int = 8192

    /** A collection of up to this many elements starts with room for all of them. A larger one starts there and grows. */
    const val COLLECTION_CHUNK: Int = 1024

    private const val CANONICAL_FLOAT_NAN: Int = 0x7fc00000
    private const val CANONICAL_DOUBLE_NAN: Long = 0x7ff8000000000000L

    @JvmStatic
    fun writeBoolean(output: DataOutput, value: Boolean) {
        output.writeByte(if (value) 1 else 0)
    }

    @JvmStatic
    fun readBoolean(input: DataInput): Boolean = when (val byte = input.readUnsignedByte()) {
        0 -> false
        1 -> true
        else -> throw MalformedDataException("a Boolean is 00 or 01, not ${hexByte(byte)}")
    }

    @JvmStatic
    fun writeByte(output: DataOutput, value: Byte) {
        output.writeByte(value.toInt())
    }

    @JvmStatic
    fun readByte(input: DataInput): Byte = input.readByte()

    @JvmStatic
    fun writeShort(output: DataOutput, value: Short) {
        output.writeShort(value.toInt())
    }

    @JvmStatic
    fun readShort(input: DataInput): Short = input.readShort()

    @JvmStatic
    fun writeChar(output: DataOutput, value: Char) {
        output.writeChar(value.code)
    }

    @JvmStatic
    fun readChar(input: DataInput): Char = input.readChar()

    @JvmStatic
    fun writeInt(output: DataOutput, value: Int) {
        output.writeInt(value)
    }

    @JvmStatic
    fun readInt(input: DataInput): Int = input.readInt()

    @JvmStatic
    fun writeLong(output: DataOutput, value: Long) {
        output.writeLong(value)
    }

    @JvmStatic
    fun readLong(input: DataInput): Long = input.readLong()

    /** `floatToIntBits`, through `writeInt`: every NaN is written as `7f c0 00 00`, on every output view. */
    @JvmStatic
    fun writeFloat(output: DataOutput, value: Float) {
        output.writeInt(java.lang.Float.floatToIntBits(value))
    }

    @JvmStatic
    fun readFloat(input: DataInput): Float {
        val bits = input.readInt()
        if (bits and 0x7fffffff > 0x7f800000 && bits != CANONICAL_FLOAT_NAN) {
            throw MalformedDataException("a Float NaN is written as 7f c0 00 00, not ${hexInt(bits)}")
        }
        return java.lang.Float.intBitsToFloat(bits)
    }

    /** `doubleToLongBits`, through `writeLong`: every NaN is written as `7f f8 00 00 00 00 00 00`, on every output view. */
    @JvmStatic
    fun writeDouble(output: DataOutput, value: Double) {
        output.writeLong(java.lang.Double.doubleToLongBits(value))
    }

    @JvmStatic
    fun readDouble(input: DataInput): Double {
        val bits = input.readLong()
        if (bits and Long.MAX_VALUE > 0x7ff0000000000000L && bits != CANONICAL_DOUBLE_NAN) {
            throw MalformedDataException("a Double NaN is written as 7f f8 00 00 00 00 00 00, not ${hexLong(bits)}")
        }
        return java.lang.Double.longBitsToDouble(bits)
    }

    /**
     * The char-varint format of Flink's `StringValue.writeString`, specified as Flinkt's own: the length plus one as
     * an unsigned base-128 varint, low group first, then each UTF-16 char as a varint of the same kind. It's lossless
     * for every `String`, unpaired surrogates included. A string over [MAX_STRING_LENGTH] chars fails before anything
     * is written, so no release writes a string it would refuse to read.
     */
    @JvmStatic
    fun writeString(output: DataOutput, value: String) {
        val length = value.length
        require(length <= MAX_STRING_LENGTH) {
            "A String of $length chars is over the budget of $MAX_STRING_LENGTH chars, so it can't be persisted"
        }
        writeVarint(output, length + 1)
        for (i in 0 until length) writeVarint(output, value[i].code)
    }

    /**
     * Reads a string [writeString] wrote. A length over [maxLength] fails before a char is read, however many follow.
     * It starts with at most [STRING_CHUNK] chars and doubles as it reads, so a corrupt length within the budget fails
     * at the end of the input instead of allocating for the length it claims.
     */
    @JvmStatic
    @JvmOverloads
    fun readString(input: DataInput, maxLength: Int = MAX_STRING_LENGTH, what: String = "a string"): String =
        readString(input, maxLength) { what }

    /**
     * [readString], with [what] called only to name a string over its budget. A reader that names each string by
     * where it is, such as a field of a declaration, then builds no name for the strings that pass.
     */
    inline fun readString(input: DataInput, maxLength: Int, what: () -> String): String {
        val length = readStringLength(input)
        if (length > maxLength) throw stringOverBudget(what(), length, maxLength)
        return readChars(input, length)
    }

    /** Consumes a string [writeString] wrote, with the checks of [readString], and allocates nothing. */
    @JvmStatic
    fun skipString(input: DataInput) {
        val length = readStringLength(input)
        if (length > MAX_STRING_LENGTH) throw stringOverBudget("a string", length, MAX_STRING_LENGTH)
        for (i in 0 until length) readVarint(input, MAX_CHAR_BYTES, Char.MAX_VALUE.code.toLong(), "a char")
    }

    /** A string's length, which the caller checks against its budget before it reads a char. */
    @PublishedApi
    internal fun readStringLength(input: DataInput): Int {
        val lengthPlusOne = readVarint(input, MAX_LENGTH_BYTES, Int.MAX_VALUE.toLong(), "a string length").toInt()
        if (lengthPlusOne == 0) {
            throw MalformedDataException(
                "a string length is written plus one, so it's never 00. Flink's StringValue writes 00 for null, " +
                    "and Flinkt writes null in a null bitmap or a null marker",
            )
        }
        return lengthPlusOne - 1
    }

    @PublishedApi
    internal fun stringOverBudget(what: String, length: Int, maxLength: Int): MalformedDataException =
        MalformedDataException("$what is $length chars long, over the budget of $maxLength")

    /** The [length] chars of a string whose length was read and checked. */
    @PublishedApi
    internal fun readChars(input: DataInput, length: Int): String {
        var chars = CharArray(minOf(length, STRING_CHUNK))
        for (i in 0 until length) {
            if (i == chars.size) chars = chars.copyOf(minOf(length, chars.size * 2))
            chars[i] = readVarint(input, MAX_CHAR_BYTES, Char.MAX_VALUE.code.toLong(), "a char").toInt().toChar()
        }
        return String(chars)
    }

    private const val MAX_CHAR_BYTES = 3
    private const val MAX_LENGTH_BYTES = 5

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
                if (count > 1 && byte == 0) throw MalformedDataException("$what has a varint with a redundant last byte 00")
                break
            }
            if (count == maxBytes) throw MalformedDataException("$what has a varint longer than $maxBytes bytes")
            shift += 7
        }
        if (value > maxValue) throw MalformedDataException("$what is $value, over $maxValue")
        return value
    }

    /** The size of a record's null bitmap: one bit per nullable field, in whole bytes. */
    @JvmStatic
    fun nullBitmapSize(nullableFields: Int): Int = (nullableFields + 7) / 8

    /**
     * Writes a null bitmap: bit `k` (byte `k / 8`, value `1 shl (k % 8)`) is 1 when the record's `k`-th nullable field,
     * in persisted order, is null. Generated code writes it with the same calls, from bits it computes inline.
     */
    @JvmStatic
    fun writeNullBitmap(output: DataOutput, nulls: BooleanArray) {
        for (byteIndex in 0 until nullBitmapSize(nulls.size)) {
            var byte = 0
            for (bit in 0 until 8) {
                val k = byteIndex * 8 + bit
                if (k < nulls.size && nulls[k]) byte = byte or (1 shl bit)
            }
            output.writeByte(byte)
        }
    }

    /** Reads a null bitmap of [nullableFields] bits. A set bit past the last nullable field is malformed. */
    @JvmStatic
    fun readNullBitmap(input: DataInput, nullableFields: Int): BooleanArray {
        val nulls = BooleanArray(nullableFields)
        for (byteIndex in 0 until nullBitmapSize(nullableFields)) {
            val byte = input.readUnsignedByte()
            for (bit in 0 until 8) {
                val k = byteIndex * 8 + bit
                val set = byte and (1 shl bit) != 0
                if (k < nullableFields) {
                    nulls[k] = set
                } else if (set) {
                    throw MalformedDataException(
                        "a null bitmap for $nullableFields nullable fields has bit $k set: ${hexByte(byte)} in its last byte",
                    )
                }
            }
        }
        return nulls
    }

    /**
     * Writes the size of a `List` or a `Map` (#16): a 4-byte big-endian `Int`. A size over [MAX_COLLECTION_SIZE]
     * fails before anything is written.
     */
    @JvmStatic
    fun writeCollectionSize(output: DataOutput, size: Int) {
        require(size <= MAX_COLLECTION_SIZE) {
            "A collection of $size elements is over the budget of $MAX_COLLECTION_SIZE, so it can't be persisted"
        }
        output.writeInt(size)
    }

    /** Reads a collection size. A negative one is malformed, and one over [MAX_COLLECTION_SIZE] fails at once. */
    @JvmStatic
    fun readCollectionSize(input: DataInput): Int {
        val size = input.readInt()
        if (size < 0) throw MalformedDataException("a collection size of $size")
        if (size > MAX_COLLECTION_SIZE) {
            throw MalformedDataException("a collection size of $size is over the budget of $MAX_COLLECTION_SIZE")
        }
        return size
    }

    /** A `List`: its size, then each element in list order (#16). */
    @JvmStatic
    fun <T> writeList(output: DataOutput, list: List<T>, element: (DataOutput, T) -> Unit) {
        writeCollectionSize(output, list.size)
        for (value in list) element(output, value)
    }

    /**
     * Reads a `List` [writeList] wrote. Nothing is allocated for an element before it's read: the list starts with
     * room for at most [COLLECTION_CHUNK] elements and grows as they're read. Every element type takes at least one
     * byte ([PersistedSchema]'s validation rejects one that takes none), so a corrupt size fails at the end of the
     * input after as many elements as the input holds.
     */
    @JvmStatic
    fun <T> readList(input: DataInput, element: (DataInput) -> T): List<T> {
        val size = readCollectionSize(input)
        val list = ArrayList<T>(minOf(size, COLLECTION_CHUNK))
        repeat(size) { list.add(element(input)) }
        return list
    }

    /**
     * A `Map`: its size, then each entry, key then value, in iteration order (#16).
     *
     * [counted] is `null` when the key type is a built-in or an enum, whose keys aren't counted. Otherwise it says
     * which keys count toward [MAX_KEYS_PER_HASH_CODE]: every key that's a record or a value class, and no enum
     * constant, object or `null` ([MapKeys.counting] decides it from the key type). A map over that budget fails
     * before anything is written, so no release writes a map it would refuse to read.
     */
    @JvmStatic
    @JvmOverloads
    fun <K, V> writeMap(
        output: DataOutput,
        map: Map<K, V>,
        key: (DataOutput, K) -> Unit,
        value: (DataOutput, V) -> Unit,
        counted: ((K) -> Boolean)? = null,
    ) {
        if (counted != null && map.size in (MAX_KEYS_PER_HASH_CODE + 1)..MAX_COLLECTION_SIZE) {
            val counts = HashCodeCounts()
            for (k in map.keys) {
                if (!counted(k)) continue
                val hash = k.hashCode()
                require(counts.add(hash) <= MAX_KEYS_PER_HASH_CODE) {
                    "A Map with more than $MAX_KEYS_PER_HASH_CODE keys whose hashCode() is $hash, such as $k, is " +
                        "over the budget of $MAX_KEYS_PER_HASH_CODE keys per hash code, so it can't be persisted"
                }
            }
        }
        writeCollectionSize(output, map.size)
        for ((k, v) in map) {
            key(output, k)
            value(output, v)
        }
    }

    /**
     * Reads a `Map` [writeMap] wrote, allocating as [readList] does. Two equal keys are malformed (#16), and so are
     * more than [MAX_KEYS_PER_HASH_CODE] [counted] keys that share a hash code. [counted] is the writer's.
     *
     * Inserting a key into the `LinkedHashMap` compares it with the earlier keys of its hash code, all of them when
     * its class isn't `Comparable`. The read fails at the first key that makes one hash code's count one too many,
     * so each insert compares a key with at most [MAX_KEYS_PER_HASH_CODE] counted keys of its hash code, plus the
     * uncounted ones: at most [MapKeys.MAX_OBJECTS] objects, which [MapKeys.counting] limits for the key type, the enum
     * constants that share its identity hash code, and `null`. Up to [MAX_KEYS_PER_HASH_CODE] keys no hash code can be
     * over, so a map that size counts nothing. Past it, each counted key's hash code is counted once more.
     */
    @JvmStatic
    @JvmOverloads
    fun <K, V> readMap(
        input: DataInput,
        key: (DataInput) -> K,
        value: (DataInput) -> V,
        counted: ((K) -> Boolean)? = null,
    ): Map<K, V> {
        val size = readCollectionSize(input)
        val map = LinkedHashMap<K, V>(minOf(size, COLLECTION_CHUNK))
        var counts: HashCodeCounts? = null
        for (i in 0 until size) {
            val k = key(input)
            map[k] = value(input)
            if (map.size != i + 1) throw MalformedDataException("a map holds the key $k twice")
            if (counted == null || map.size <= MAX_KEYS_PER_HASH_CODE) continue
            // The first time the map is larger than the budget, the keys before k are counted, and from then on each
            // key as it's read. The insert above compared k with at most the budget's keys of its hash code.
            val table = counts ?: HashCodeCounts().also { first ->
                counts = first
                for (earlier in map.keys) if (earlier !== k && counted(earlier)) first.add(earlier.hashCode())
            }
            if (counted(k) && table.add(k.hashCode()) > MAX_KEYS_PER_HASH_CODE) {
                throw MalformedDataException(
                    "a map holds more than $MAX_KEYS_PER_HASH_CODE keys whose hashCode() is ${k.hashCode()}, such as " +
                        "$k, over the budget of $MAX_KEYS_PER_HASH_CODE keys per hash code",
                )
            }
        }
        return map
    }

    /**
     * How many keys have each hash code. Its own lookups stay logarithmic however the hash codes collide: it's keyed
     * by boxed `Int`s, which are `Comparable`, so `HashMap` orders a bin of them by `compareTo` instead of searching
     * it. It holds an entry per distinct hash code, so at most a constant factor of the map it counts.
     */
    private class HashCodeCounts {
        private val counts = HashMap<Int, IntArray>()

        /** Counts one more key with [hash], and returns how many there are now. */
        fun add(hash: Int): Int = ++counts.getOrPut(hash) { IntArray(1) }[0]
    }

    /** The marker before a nullable value outside a record field: `00` for null, `01` before a value. */
    @JvmStatic
    fun writeNullMarker(output: DataOutput, isNull: Boolean) {
        output.writeByte(if (isNull) 0 else 1)
    }

    /** Whether the marker says a value follows. */
    @JvmStatic
    fun readNullMarker(input: DataInput): Boolean = when (val byte = input.readUnsignedByte()) {
        0 -> false
        1 -> true
        else -> throw MalformedDataException("a null marker is 00 or 01, not ${hexByte(byte)}")
    }

    /**
     * The failure for a `00` marker where a value must follow: a present field declared as `T?` whose argument is
     * nullable. Its bitmap bit already says it isn't null, so `00` would be a second encoding of null that no writer
     * produces (docs/persisted-format.md, "Null representation").
     */
    @JvmStatic
    fun nullInPresentField(field: String): MalformedDataException = MalformedDataException(
        "$field is declared as a nullable type parameter and its null bitmap bit is clear, so it holds a value, " +
            "and its argument's null marker is 01, not 00",
    )

    private fun hexByte(byte: Int): String = "%02x".format(byte)

    private fun hexInt(bits: Int): String = "%08x".format(bits).chunked(2).joinToString(" ")

    private fun hexLong(bits: Long): String = "%016x".format(bits).chunked(2).joinToString(" ")
}

/** Bytes that no value encodes: a corrupt or misread record or snapshot. */
class MalformedDataException(message: String) : IOException(message)
