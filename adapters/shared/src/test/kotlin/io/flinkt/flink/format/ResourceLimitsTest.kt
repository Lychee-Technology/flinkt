package io.flinkt.flink.format

import io.flinkt.schema.DeclarationSchema
import io.flinkt.schema.EnumConstantSchema
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.schema.SchemaTypeArgument
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.SealedSubtypeSchema
import io.flinkt.schema.ValueClassSchema
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataInputStream
import java.io.EOFException
import java.io.InputStream
import java.io.SequenceInputStream
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import org.apache.flink.core.memory.DataInputDeserializer
import org.apache.flink.core.memory.DataOutputSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * The resource limits of record format version 1 (docs/persisted-format.md, "Resource limits"): the string and
 * collection budgets, which the writer enforces too, what a corrupt length can make a reader allocate, the rule that
 * makes every turn of a loop over a collection consume input, and the old-layout walker's work, its duplicate-key check
 * included.
 */
class ResourceLimitsTest {
    private val max = RecordFormat.MAX_COLLECTION_SIZE

    @Test
    fun `a string over its budget fails at its length, though every char it claims follows, and is never written`() {
        val over = RecordFormat.MAX_STRING_LENGTH + 1
        val reads = listOf<(DataInput) -> Unit>({ RecordFormat.readString(it) }, { RecordFormat.skipString(it) })
        for (read in reads) {
            // The length plus one, then "a" for ever: the input holds every char the length claims.
            val input = endless(varint(over + 1), 0x61)
            lateinit var e: MalformedDataException
            val allocated = allocatedBy { e = assertThrows<MalformedDataException> { read(input) } }
            assertEquals("a string is 67108865 chars long, over the budget of 67108864", e.message)
            assertTrue(allocated < 1_000_000, "reading allocated $allocated bytes")
        }
        // A length of the budget is accepted, and a missing char is the end of the input.
        assertThrows<EOFException> {
            RecordFormat.readString(DataInputDeserializer(varint(RecordFormat.MAX_STRING_LENGTH + 1) + bytes("61 61")))
        }

        val output = DataOutputSerializer(16)
        val tooLong = String(ByteArray(over).also { it.fill(0x61) }, Charsets.ISO_8859_1)
        val e = assertThrows<IllegalArgumentException> { RecordFormat.writeString(output, tooLong) }
        assertEquals("A String of 67108865 chars is over the budget of 67108864 chars, so it can't be persisted", e.message)
        assertEquals(0, output.length())
    }

    @Test
    fun `a collection over its budget fails at its size, and is never written`() {
        fun fails(size: Int, message: String) {
            val input = DataInputDeserializer(int(size) + ByteArray(16))
            val e = assertThrows<MalformedDataException> { RecordFormat.readList(input, RecordFormat::readLong) }
            assertEquals(message, e.message)
            assertEquals(16, input.available(), "nothing after the size was read")
        }
        fails(max + 1, "a collection size of 67108865 is over the budget of 67108864")
        fails(Int.MAX_VALUE, "a collection size of 2147483647 is over the budget of 67108864")
        fails(-1, "a collection size of -1")

        val huge = object : AbstractList<Long>() {
            override val size: Int = max + 1

            override fun get(index: Int): Long = 0L
        }
        val output = DataOutputSerializer(16)
        val e = assertThrows<IllegalArgumentException> { RecordFormat.writeList(output, huge, RecordFormat::writeLong) }
        assertEquals("A collection of 67108865 elements is over the budget of 67108864, so it can't be persisted", e.message)
        assertEquals(0, output.length())
    }

    @Test
    fun `lists and maps are their size, then their elements or entries`() {
        val list = written { RecordFormat.writeList(it, listOf(1L, 2L), RecordFormat::writeLong) }
        assertEquals(normalized("00 00 00 02 | 00 00 00 00 00 00 00 01 | 00 00 00 00 00 00 00 02"), list.hex())
        assertEquals(listOf(1L, 2L), RecordFormat.readList(DataInputDeserializer(list.bytes), RecordFormat::readLong))

        val map = written { RecordFormat.writeMap(it, mapOf("b" to 1, "a" to 2), { out, k -> RecordFormat.writeString(out, k) }, RecordFormat::writeInt) }
        assertEquals(normalized("00 00 00 02 | 02 62 00 00 00 01 | 02 61 00 00 00 02"), map.hex())
        val read = RecordFormat.readMap(DataInputDeserializer(map.bytes), { RecordFormat.readString(it) }, RecordFormat::readInt)
        assertEquals(listOf("b" to 1, "a" to 2), read.toList())

        // Two equal keys would merge two entries silently.
        val twice = bytes("00 00 00 02 | 02 61 00 00 00 01 | 02 61 00 00 00 02")
        val e = assertThrows<MalformedDataException> {
            RecordFormat.readMap(DataInputDeserializer(twice), { RecordFormat.readString(it) }, RecordFormat::readInt)
        }
        assertEquals("a map holds the key a twice", e.message)
    }

    @Test
    fun `a corrupt size within the budget allocates only for the elements the input holds`() {
        val claim = int(max)
        // Three of the 67,108,864 Longs the size claims.
        val longs = claim + long(1) + long(2) + long(3)
        // Each list claims the budget. The outer one holds one list, which holds one Long.
        val nested = claim + claim + long(1)
        // One of the entries the size claims.
        val entries = claim + bytes("02 61") + long(1)
        val reads = listOf<Pair<String, () -> Unit>>(
            "List<Long>" to { RecordFormat.readList(DataInputDeserializer(longs), RecordFormat::readLong) },
            "List<List<Long>>" to {
                RecordFormat.readList(DataInputDeserializer(nested)) { RecordFormat.readList(it, RecordFormat::readLong) }
            },
            "Map<String, Long>" to {
                RecordFormat.readMap(DataInputDeserializer(entries), { RecordFormat.readString(it) }, RecordFormat::readLong)
            },
        )
        for ((what, read) in reads) {
            val allocated = allocatedBy { assertThrows<EOFException>(what) { read() } }
            assertTrue(allocated < 1_000_000, "$what: reading allocated $allocated bytes")
        }
    }

    @Test
    fun `a type written as no bytes is only a serialized type or a sealed subtype, for the writer and the reader`() {
        val declarations: Map<String, DeclarationSchema> = listOf(
            ObjectSchema("com.example.Shutdown"),
            RecordSchema("com.example.Empty", emptyList()),
            RecordSchema("com.example.Box", listOf(FieldSchema("value", SchemaType.TypeParameter("T"))), listOf("T")),
            RecordSchema("com.example.Holder", listOf(FieldSchema("marker", type("com.example.Shutdown", nullable = true)))),
            SealedSchema(
                "com.example.Event",
                listOf(SealedSubtypeSchema(1, type("com.example.Shutdown")), SealedSubtypeSchema(2, type("com.example.Empty"))),
            ),
        ).associateBy { it.typeName }
        val because = "as a field, an element or a type argument, and its values are written as no bytes. Only the " +
            "serialized type or a sealed subtype may be, so that reading every other value consumes input"

        fun rejected(root: SchemaType.ClassType, message: String) {
            val e = assertThrows<IllegalArgumentException>("$root") { PersistedSchema.of(root) { declarations[it] } }
            assertEquals(message, e.message)
        }
        // A List<Shutdown> of any size would be four bytes, and a reader would loop over its size consuming nothing.
        rejected(list(type("com.example.Shutdown")), "the serialized type uses com.example.Shutdown $because")
        rejected(
            SchemaType.ClassType("kotlin.collections.Map", listOf(SchemaTypeArgument.invariant(type("kotlin.String")), SchemaTypeArgument.invariant(type("com.example.Empty")))),
            "the serialized type uses com.example.Empty $because",
        )
        rejected(type("com.example.Holder"), "com.example.Holder uses com.example.Shutdown $because")
        rejected(
            SchemaType.ClassType("com.example.Box", listOf(SchemaTypeArgument.invariant(type("com.example.Empty")))),
            "the serialized type uses com.example.Empty $because",
        )

        // As the serialized type, or as a subtype after its ID, it's allowed.
        for (root in listOf("com.example.Shutdown", "com.example.Empty", "com.example.Event")) {
            PersistedSchema.of(type(root)) { declarations[it] }
        }

        // The reader refuses the same schema from a snapshot, so a removed List<Shutdown> is never walked.
        val listOfShutdown = Bytes.of {
            writeByte(1); string("kotlin.collections.List"); writeByte(0); writeInt(1)
            writeByte(1); write(Bytes.type("com.example.Shutdown"))
        }
        val snapshot = Bytes.snapshot(listOfShutdown, Bytes.of { writeByte(5); string("com.example.Shutdown"); writeInt(0) })
        val e = assertThrows<MalformedDataException> { PersistedSchema.read(DataInputDeserializer(snapshot)) }
        assertEquals("the snapshot holds an invalid schema: the serialized type uses com.example.Shutdown $because", e.message)
    }

    @Test
    fun `the old-layout walker consumes input on every turn of a collection, allocates nothing, and stops at the budget`() {
        // A removed List<Long?>: each element takes at least its null marker.
        val root = list(type("kotlin.Long", nullable = true))
        val walker = SchemaWalker(PersistedSchema.of(root) { null })

        // The size claims the budget, and a mebibyte of null elements follows: every byte is consumed, then the
        // input ends.
        val input = DataInputDeserializer(int(max) + ByteArray(1 shl 20))
        val allocated = allocatedBy { assertThrows<EOFException> { walker.skipValue(root, input) } }
        assertEquals(0, input.available())
        assertTrue(allocated < 1_000_000, "walking allocated $allocated bytes")

        val over = DataInputDeserializer(int(max + 1) + ByteArray(16))
        val e = assertThrows<MalformedDataException> { walker.skipValue(root, over) }
        assertEquals("a collection size of 67108865 is over the budget of 67108864", e.message)
        assertEquals(16, over.available())
    }

    @Test
    fun `the old-layout walker keeps only a map's keys' bytes, a constant factor of the bytes it consumed`() {
        // A removed Map<Int, Long?> whose size claims the budget, then a mebibyte of distinct entries, each a key and
        // a null marker: every byte is consumed, then the input ends. To find a key that's there twice, the walker
        // keeps each key's bytes, and nothing else.
        val root = map(type("kotlin.Int"), type("kotlin.Long", nullable = true))
        val walker = SchemaWalker(PersistedSchema.of(root) { null })
        val data = int(max) + Bytes.of { repeat((1 shl 20) / 5) { writeInt(it); writeByte(0) } }
        val input = DataInputDeserializer(data)
        val allocated = allocatedBy { assertThrows<EOFException> { walker.skipValue(root, input) } }
        assertEquals(0, input.available())
        assertTrue(allocated < 64L * data.size, "walking ${data.size} bytes allocated $allocated")
    }

    @Test
    fun `a removed map's duplicate-key check finds a key that's there twice among many`() {
        // 2^16 distinct keys that share a 32-char prefix, then the first one again: the codec's reader and the walker
        // read every key, and both fail on the last.
        val keys = 1 shl 16
        fun key(i: Int) = "k".repeat(32) + i.toString(36)
        fun entries(vararg extra: String) = Bytes.of {
            writeInt(keys + extra.size)
            for (i in 0 until keys) { string(key(i)); writeByte(0) }
            for (k in extra) { string(k); writeByte(0) }
        }
        val root = map(type("kotlin.String"), type("kotlin.Long", nullable = true))
        val walker = SchemaWalker(PersistedSchema.of(root) { null })
        fun codec(bytes: ByteArray) = RecordFormat.readMap(
            DataInputDeserializer(bytes),
            { RecordFormat.readString(it) },
            { if (RecordFormat.readNullMarker(it)) RecordFormat.readLong(it) else null },
        )
        val distinct = entries()
        assertEquals(keys, codec(distinct).size)
        val input = DataInputDeserializer(distinct)
        walker.skipValue(root, input)
        assertEquals(0, input.available())

        val twice = entries(key(0))
        assertEquals("a map holds the key ${key(0)} twice", assertThrows<MalformedDataException> { codec(twice) }.message)
        val e = assertThrows<MalformedDataException> { walker.skipValue(root, DataInputDeserializer(twice)) }
        assertEquals("a map holds the key written as 22 6b 6b 6b 6b 6b 6b 6b 6b 6b 6b 6b 6b 6b 6b 6b … (34 bytes) twice", e.message)
    }

    @Test
    fun `a map holds at most 1,024 keys that share a hash code, when it's read and when it's written`() {
        // 16,000 keys Point(i, -31 * i) of a data class with two Int properties, which all hash to 0, each with a
        // Boolean value: 144,004 bytes. The read fails at the 1,025th key, and reads nothing after it.
        val count = 16_000
        assertEquals(0, Point(5, -155).hashCode())
        val colliding = points(count) { Point(it, -31 * it) }
        assertEquals(144_004, colliding.size)
        val input = DataInputDeserializer(colliding)
        val e = assertThrows<MalformedDataException> { readPoints(input) }
        assertEquals(
            "a map holds more than 1024 keys whose hashCode() is 0, such as Point(a=1024, b=-31744), over the budget " +
                "of 1024 keys per hash code",
            e.message,
        )
        assertEquals(colliding.size - 4 - 1025 * 9, input.available())

        // 1,024 keys with one hash code are within the budget, among keys with other hash codes.
        val within = points(count) { if (it < 1024) Point(it, -31 * it) else Point(it, it) }
        assertEquals(count, readPoints(DataInputDeserializer(within)).size)

        // The writer refuses a map one key over the budget before it writes a byte, and writes one at the budget.
        val output = DataOutputSerializer(16)
        val over = (0..1024).associate { Point(it, -31 * it) to true }
        val written = assertThrows<IllegalArgumentException> { writePoints(output, over) }
        assertEquals(
            "A Map with more than 1024 keys whose hashCode() is 0, such as Point(a=1024, b=-31744), is over the " +
                "budget of 1024 keys per hash code, so it can't be persisted",
            written.message,
        )
        assertEquals(0, output.length())
        val atBudget = (0 until 1024).associate { Point(it, -31 * it) to true }
        writePoints(output, atBudget)
        assertEquals(atBudget, readPoints(DataInputDeserializer(output.copyOfBuffer)))
    }

    @Test
    fun `the budget bounds the comparisons a map's read makes, which grow with the square of its size without it`() {
        // Keys that hash as Point does and count the equals() calls the map makes. Without the budget, a read of n
        // keys with one hash code compares each with every earlier one: n * (n - 1) / 2 calls, and a few per key on
        // the way down the bin's tree.
        fun read(bytes: ByteArray, counted: ((CountedKey) -> Boolean)?): Map<CountedKey, Boolean> =
            RecordFormat.readMap(DataInputDeserializer(bytes), { CountedKey(it.readInt(), it.readInt()) }, RecordFormat::readBoolean, counted)
        fun comparisons(block: () -> Unit): Long {
            CountedKey.comparisons = 0
            block()
            return CountedKey.comparisons
        }
        for (n in listOf(4_000, 8_000, 16_000)) {
            val unchecked = comparisons { assertEquals(n, read(points(n) { Point(it, -31 * it) }, counted = null).size) }
            val pairs = n.toLong() * (n - 1) / 2
            assertTrue(unchecked in pairs until pairs + 64L * n, "an unchecked read of $n keys made $unchecked comparisons")
        }
        val colliding = points(16_000) { Point(it, -31 * it) }

        // With the budget, the read fails at the 1,025th key, after about 1,025 * 1,024 / 2.
        val checked = comparisons { assertThrows<MalformedDataException> { read(colliding) { true } } }
        assertTrue(checked < 1025L * 1025, "a checked read made $checked comparisons")

        // 16 hash codes with 1,024 keys each are within the budget: each key is compared with at most 1,024 others.
        val sixteen = points(16 * 1024) { Point(it / 1024 + it % 1024, -31 * (it % 1024)) }
        val full = comparisons { assertEquals(16 * 1024, read(sixteen) { true }.size) }
        assertTrue(full < 16L * 1024 * 1024, "16,384 keys at the budget made $full comparisons")

        // Keys that aren't counted, as an enum constant or an object of a sealed key type isn't, don't count toward it.
        val uncounted = Bytes.of {
            writeInt(1024 + 3)
            for (i in 0 until 1024 + 3) { writeInt(i); writeInt(-31 * i); writeByte(1) }
        }
        val map = RecordFormat.readMap(
            DataInputDeserializer(uncounted),
            { CountedKey(it.readInt(), it.readInt()) },
            RecordFormat::readBoolean,
        ) { it.a < 1024 }
        assertEquals(1024 + 3, map.size)
    }

    @Test
    fun `a built-in's keys that share a hash code cost each key a logarithmic number of comparisons, so they aren't counted`() {
        // HashMap orders keys of a class that's Comparable to itself, as String and the boxed built-ins are, by
        // compareTo when they share a hash code. 2^14 Strings of "Aa" and "BB", which have one hash code, and keys
        // that count their comparisons, Comparable but otherwise like CountedKey.
        val strings = (0 until (1 shl 14)).map { i -> (0 until 14).joinToString("") { if (i shr it and 1 == 0) "Aa" else "BB" } }
        assertEquals(1, strings.map { it.hashCode() }.toSet().size)
        val bytes = Bytes.of { writeInt(strings.size); for (s in strings) { string(s); writeByte(1) } }
        val read = RecordFormat.readMap(DataInputDeserializer(bytes), { RecordFormat.readString(it) }, RecordFormat::readBoolean)
        assertEquals(strings, read.keys.toList())

        val ordered = points(1 shl 14) { Point(it, -31 * it) }
        OrderedKey.comparisons = 0
        val map = RecordFormat.readMap(DataInputDeserializer(ordered), { OrderedKey(it.readInt(), it.readInt()) }, RecordFormat::readBoolean)
        assertEquals(1 shl 14, map.size)
        assertTrue(OrderedKey.comparisons < (1L shl 14) * 64, "2^14 ordered keys made ${OrderedKey.comparisons} comparisons")
    }

    @Test
    fun `a key's hash code is the same where it's written and where it's read, so no enum or object is inside a record of it`() {
        // A record's generated hashCode() combines its components' hash codes. An enum constant's is an identity hash
        // code, which another JVM can compute differently, so a writer's count of such keys isn't its reader's.
        assertEquals(31 * System.identityHashCode(Color.RED) + 5, Tagged(Color.RED, 5).hashCode())

        val declarations: Map<String, DeclarationSchema> = listOf(
            EnumSchema("com.example.Color", listOf(EnumConstantSchema("RED", 1))),
            ObjectSchema("com.example.Dot"),
            RecordSchema("com.example.Point", listOf(FieldSchema("a", type("kotlin.Int")), FieldSchema("b", type("kotlin.Int")))),
            RecordSchema("com.example.Circle", listOf(FieldSchema("r", type("kotlin.Int")))),
            SealedSchema("com.example.Shape", listOf(SealedSubtypeSchema(1, type("com.example.Circle")), SealedSubtypeSchema(2, type("com.example.Dot")))),
            SealedSchema("com.example.Round", listOf(SealedSubtypeSchema(1, type("com.example.Circle")))),
            RecordSchema("com.example.Tagged", listOf(FieldSchema("color", type("com.example.Color", nullable = true)), FieldSchema("n", type("kotlin.Int")))),
            RecordSchema("com.example.Holder", listOf(FieldSchema("shape", type("com.example.Shape")))),
            RecordSchema("com.example.RoundHolder", listOf(FieldSchema("round", type("com.example.Round")))),
            RecordSchema("com.example.Box", listOf(FieldSchema("value", SchemaType.TypeParameter("T"))), listOf("T")),
            RecordSchema("com.example.Listed", listOf(FieldSchema("points", list(type("kotlin.Int"))))),
            ValueClassSchema("com.example.UserId", FieldSchema("id", type("kotlin.Long"))),
            ValueClassSchema("com.example.Tag", FieldSchema("color", type("com.example.Color"))),
            SealedSchema("com.example.Labelled", listOf(SealedSubtypeSchema(1, type("com.example.Tagged")))),
        ).associateBy { it.typeName }
        fun counting(key: SchemaType.ClassType) = MapKeys.counting(key) { declarations[it] }
        fun box(argument: SchemaType) = SchemaType.ClassType("com.example.Box", listOf(SchemaTypeArgument.invariant(argument)))

        for (key in listOf(type("kotlin.String"), type("kotlin.Long", nullable = true), type("com.example.Color"), type("com.example.Color", nullable = true))) {
            assertEquals(MapKeys.Counting.NONE, counting(key), "$key")
        }
        // A record, a value class and a sealed type whose object subtype is the key itself are counted.
        for (key in listOf(type("com.example.Point"), type("com.example.UserId"), type("com.example.Shape"), type("com.example.RoundHolder"), box(type("com.example.Point")))) {
            assertEquals(MapKeys.Counting.RECORDS_AND_VALUE_CLASSES, counting(key), "$key")
        }

        val because = "hashCode() is an identity hash code, which another JVM can compute differently, and a map's keys " +
            "are counted by hash code where it's written and where it's read. An enum or an object may be the key type " +
            "itself, or a subtype of a sealed key type"
        fun rejected(key: SchemaType.ClassType, message: String) {
            val e = assertThrows<IllegalArgumentException>("$key") { counting(key) }
            assertEquals(message, e.message)
        }
        rejected(type("com.example.Tagged"), "com.example.Tagged can't be a Map key: it holds the enum com.example.Color at com.example.Tagged.color. An enum constant's $because")
        rejected(type("com.example.Tag"), "com.example.Tag can't be a Map key: it holds the enum com.example.Color at com.example.Tag.color. An enum constant's $because")
        rejected(box(type("com.example.Color")), "com.example.Box<com.example.Color> can't be a Map key: it holds the enum com.example.Color at com.example.Box.value. An enum constant's $because")
        rejected(
            type("com.example.Holder"),
            "com.example.Holder can't be a Map key: it holds com.example.Dot, an object subtype of com.example.Shape, at com.example.Holder.shape. An object's $because",
        )
        rejected(type("com.example.Labelled"), "com.example.Labelled can't be a Map key: it holds the enum com.example.Color at com.example.Tagged.color. An enum constant's $because")
        rejected(type("com.example.Listed"), "com.example.Listed can't be a Map key: it holds a kotlin.collections.List at com.example.Listed.points, and no key type holds a collection")
    }

    private data class Point(val a: Int, val b: Int)

    private enum class Color { RED }

    private data class Tagged(val color: Color, val n: Int)

    /** Hashes as [Point] does, and counts the `equals()` calls a map makes. */
    private class CountedKey(val a: Int, val b: Int) {
        override fun equals(other: Any?): Boolean {
            comparisons++
            return other is CountedKey && other.a == a && other.b == b
        }

        override fun hashCode(): Int = 31 * a + b

        override fun toString(): String = "CountedKey($a, $b)"

        companion object {
            var comparisons = 0L
        }
    }

    /** [CountedKey], but `Comparable` to itself, so `HashMap` orders keys that share a hash code. */
    private class OrderedKey(val a: Int, val b: Int) : Comparable<OrderedKey> {
        override fun compareTo(other: OrderedKey): Int {
            comparisons++
            return compareValuesBy(this, other, { it.a }, { it.b })
        }

        override fun equals(other: Any?): Boolean {
            comparisons++
            return other is OrderedKey && other.a == a && other.b == b
        }

        override fun hashCode(): Int = 31 * a + b

        companion object {
            var comparisons = 0L
        }
    }

    /** A `Map<Point, Boolean>` of [count] keys, each `true`. */
    private fun points(count: Int, point: (Int) -> Point): ByteArray = Bytes.of {
        writeInt(count)
        for (i in 0 until count) {
            val p = point(i)
            writeInt(p.a); writeInt(p.b); writeByte(1)
        }
    }

    private fun readPoints(input: DataInput): Map<Point, Boolean> =
        RecordFormat.readMap(input, { Point(it.readInt(), it.readInt()) }, RecordFormat::readBoolean) { true }

    private fun writePoints(output: DataOutputSerializer, map: Map<Point, Boolean>) = RecordFormat.writeMap(
        output,
        map,
        { out, p -> RecordFormat.writeInt(out, p.a); RecordFormat.writeInt(out, p.b) },
        RecordFormat::writeBoolean,
    ) { true }

    private fun type(name: String, nullable: Boolean = false) = SchemaType.ClassType(name, isNullable = nullable)

    private fun list(element: SchemaType) =
        SchemaType.ClassType("kotlin.collections.List", listOf(SchemaTypeArgument.invariant(element)))

    private fun map(key: SchemaType, value: SchemaType) = SchemaType.ClassType(
        "kotlin.collections.Map",
        listOf(SchemaTypeArgument.invariant(key), SchemaTypeArgument.invariant(value)),
    )

    private fun int(value: Int): ByteArray = ByteBuffer.allocate(4).putInt(value).array()

    private fun long(value: Long): ByteArray = ByteBuffer.allocate(8).putLong(value).array()

    /** An unsigned varint, as the string format writes a length plus one. */
    private fun varint(value: Int): ByteArray {
        val bytes = ByteArrayOutputStream()
        var rest = value
        while (rest >= 0x80) {
            bytes.write(rest and 0x7f or 0x80)
            rest = rest ushr 7
        }
        bytes.write(rest)
        return bytes.toByteArray()
    }

    /** [prefix], then the byte [fill] for ever: an input that holds every byte a length claims. */
    private fun endless(prefix: ByteArray, fill: Int): DataInput =
        DataInputStream(SequenceInputStream(ByteArrayInputStream(prefix), object : InputStream() {
            override fun read(): Int = fill
        }))
}

/** The bytes this thread allocates while [block] runs. */
internal fun allocatedBy(block: () -> Unit): Long {
    val threads = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
    val before = threads.getThreadAllocatedBytes(Thread.currentThread().id)
    block()
    return threads.getThreadAllocatedBytes(Thread.currentThread().id) - before
}
