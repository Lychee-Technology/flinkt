package io.flinkt.flink.format

import io.flinkt.schema.DeclarationSchema
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.schema.SchemaTypeArgument
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.SealedSubtypeSchema
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
 * makes every turn of a loop over a collection consume input, and the old-layout walker's work.
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

    private fun type(name: String, nullable: Boolean = false) = SchemaType.ClassType(name, isNullable = nullable)

    private fun list(element: SchemaType) =
        SchemaType.ClassType("kotlin.collections.List", listOf(SchemaTypeArgument.invariant(element)))

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

    /** The bytes this thread allocates while [block] runs. */
    private fun allocatedBy(block: () -> Unit): Long {
        val threads = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
        val before = threads.getThreadAllocatedBytes(Thread.currentThread().id)
        block()
        return threads.getThreadAllocatedBytes(Thread.currentThread().id) - before
    }
}
