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
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.time.Duration
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.DataInputDeserializer
import org.apache.flink.core.memory.DataOutputSerializer
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Snapshot format version 1 (docs/persisted-format.md, "Snapshot"): the bytes of `User`'s snapshot, a round trip of a
 * schema with every planned shape, and a reader that rejects malformed data and data over its budgets.
 */
class SnapshotFormatTest {
    private val userSchema = RecordSchema("com.example.User", listOf(field("id", "kotlin.Long"), field("name", "kotlin.String")))

    @Test
    fun `User's snapshot`() {
        val schema = PersistedSchema.of(type("com.example.User")) { mapOf("com.example.User" to userSchema)[it] }
        assertEquals(normalized(annotatedHex(USER_SNAPSHOT)), hex(schema.encoded))
        assertEquals(120, schema.encoded.size)

        // Flink frames it: the snapshot class's name, the snapshot format version, then the data.
        val framed = DataOutputSerializer(64)
        TypeSerializerSnapshot.writeVersionedSnapshot(framed, PrototypeSerializerSnapshot<Any>(schema))
        val name = PrototypeSerializerSnapshot::class.java.name
        val expected = DataOutputSerializer(64).apply {
            writeUTF(name)
            writeInt(1)
            write(schema.encoded)
            writeInt(0) // the adapter's part: no nested Flink snapshot
        }
        assertArrayEquals(expected.copyOfBuffer, framed.copyOfBuffer)
    }

    @Test
    fun `a schema with every planned shape round-trips, each declaration once`() {
        for (role in KeyRole.entries) {
            val schema = PersistedSchema.of(envelopeOfUser, role) { everyShape[it] }
            assertEquals(everyShape.keys.sorted(), schema.declarations.keys.toList())
            val read = PersistedSchema.read(DataInputDeserializer(schema.encoded))
            assertEquals(role, read.role)
            assertEquals(1, read.recordFormatVersion)
            assertEquals(envelopeOfUser, read.root)
            assertEquals(schema.declarations, read.declarations)
            assertArrayEquals(schema.encoded, read.encoded)
        }
    }

    @Test
    fun `constants and subtypes are written in ID order, and fields in constructor order`() {
        val schema = PersistedSchema.of(envelopeOfUser) { everyShape[it] }
        assertEquals(listOf(1, 2, 7), (schema.declaration("com.example.Color") as EnumSchema).constants.map { it.id })
        assertEquals(listOf(1, 2), (schema.declaration("com.example.Event") as SealedSchema).subtypes.map { it.id })

        // Declaring the constants in another order changes no snapshot byte.
        val redeclared = everyShape + ("com.example.Color" to EnumSchema("com.example.Color", colors.reversed()))
        assertArrayEquals(schema.encoded, PersistedSchema.of(envelopeOfUser) { redeclared[it] }.encoded)

        // A constructor reorder is recorded: it changes the snapshot, and not the layout.
        val reordered = everyShape + ("com.example.User" to RecordSchema("com.example.User", userSchema.fields.reversed()))
        val other = PersistedSchema.of(envelopeOfUser) { reordered[it] }
        assertNotEquals(hex(schema.encoded), hex(other.encoded))
        assertTrue(schema.sameLayout(other, compareConstructorOrder = false))
        assertTrue(!schema.sameLayout(other, compareConstructorOrder = true))
    }

    @Test
    fun `a reader driven by the schema consumes a value of every planned shape but value classes`() {
        val schema = PersistedSchema.of(envelopeOfUser) { (everyShape - "com.example.UserId" + userCreatedWithLong)[it] }
        // Envelope<User>(payload = User(1, "a"), meta = {"k": [null, UserCreated(5, GREEN), Shutdown]}, previous = null)
        val value = bytes(
            """
            01                                  Envelope's bitmap: previous is null
            00 00 00 01 | 02 6b                 meta: one entry, key "k"
            00 00 00 03                         the List<Event?> value: three elements
            00                                  null
            01 00 00 00 01 00 00 00 01          an Event: UserEvent (1), then UserCreated (1)
            00 00 00 02 00 00 00 00 00 00 00 05 UserCreated: color GREEN (2), userId 5
            01 00 00 00 02                      an Event: Shutdown (2), an object, so no payload
            00 00 00 00 00 00 00 01 02 61       payload: User(1, "a")
            ee                                  the next byte
            """.let(::annotatedHex),
        )
        val input = DataInputDeserializer(value)
        SchemaWalker(schema).skipValue(envelopeOfUser, input)
        assertEquals(0xee, input.readUnsignedByte())

        fun fails(patch: (ByteArray) -> Unit, message: String) {
            val corrupt = value.copyOf().also(patch)
            val e = assertThrows<MalformedDataException> { SchemaWalker(schema).skipValue(envelopeOfUser, DataInputDeserializer(corrupt)) }
            assertEquals(message, e.message)
        }
        fails({ it[20] = 9 }, "com.example.UserEvent has no subtype with ID 9")
        fails({ it[24] = 3 }, "com.example.Color has no constant with ID 3")
        fails({ it[7] = 0xff.toByte() }, "a collection size of -16777213")
        fails({ it[12] = 2 }, "a null marker is 00 or 01, not 02")
    }

    @Test
    fun `a present field declared T? whose argument is nullable has the marker 01, and 00 there fails`() {
        val root = SchemaType.ClassType("com.example.Envelope", listOf(SchemaTypeArgument.invariant(type("com.example.User", nullable = true))))
        val schema = PersistedSchema.of(root) { (everyShape - "com.example.UserId" + userCreatedWithLong)[it] }
        // Envelope<User?>(payload = null, meta = {}, previous = User(2, "b"))
        val value = bytes(
            """
            00                                  Envelope's bitmap: previous is present
            00 00 00 00                         meta: no entries
            00                                  payload, declared T: the argument's marker, null
            01 00 00 00 00 00 00 00 02 02 62    previous, declared T?: the argument's marker, then User(2, "b")
            """.let(::annotatedHex),
        )
        val input = DataInputDeserializer(value)
        SchemaWalker(schema).skipValue(root, input)
        assertEquals(0, input.available())

        // A clear bit, then 00, would be a second encoding of a null previous, which sets the bit.
        val corrupt = value.copyOf().also { it[6] = 0 }
        val e = assertThrows<MalformedDataException> { SchemaWalker(schema).skipValue(root, DataInputDeserializer(corrupt)) }
        assertEquals(
            "com.example.Envelope.previous is declared as a nullable type parameter and its null bitmap bit is " +
                "clear, so it holds a value, and its argument's null marker is 01, not 00",
            e.message,
        )
    }

    @Test
    fun `the reader rejects malformed snapshots, naming what's wrong`() {
        fun fails(bytes: ByteArray, message: String) {
            val e = assertThrows<MalformedDataException> { PersistedSchema.read(DataInputDeserializer(bytes)) }
            assertTrue(message in e.message.orEmpty(), "expected \"$message\" in \"${e.message}\"")
        }
        val user = Bytes.record("com.example.User", "id" to Bytes.type("kotlin.Long"), "name" to Bytes.type("kotlin.String"))

        fails(Bytes.snapshotWithRole(3, Bytes.type("com.example.User"), user), "unknown key role 3")
        fails(Bytes.snapshot(Bytes.type("com.example.User"), Bytes.of { writeByte(9); string("com.example.User") }), "declaration kind 9")
        fails(Bytes.snapshot(Bytes.of { writeByte(7) }), "type kind 7")
        fails(Bytes.snapshot(Bytes.of { writeByte(1); string("kotlin.collections.List"); writeByte(0); writeInt(1); writeByte(4) }), "unknown variance 4")
        fails(Bytes.snapshot(Bytes.of { writeByte(1); string("kotlin.Long"); writeByte(2) }), "a Boolean is 00 or 01, not 02")
        fails(Bytes.snapshot(Bytes.of { writeByte(2); string("T"); writeByte(0) }), "the serialized type is a type parameter")

        val a = Bytes.record("com.example.A", "b" to Bytes.type("com.example.B"))
        val b = Bytes.record("com.example.B", "a" to Bytes.type("com.example.A"))
        fails(Bytes.snapshot(Bytes.type("com.example.A"), b, a), "declaration com.example.A follows com.example.B")
        fails(Bytes.snapshot(Bytes.type("com.example.User"), user, user), "declaration com.example.User follows com.example.User")
        fails(Bytes.snapshot(Bytes.type("com.example.A"), a, b), "com.example.A -> com.example.B -> com.example.A: recursive types aren't supported")
        fails(
            Bytes.snapshot(Bytes.type("com.example.User"), Bytes.record("com.example.User", "profile" to Bytes.type("com.example.Profile"))),
            "com.example.Profile is neither a built-in, a collection nor a declaration in the snapshot",
        )
        fails(Bytes.snapshot(Bytes.type("com.example.User"), user, Bytes.record("com.example.Zed")), "the snapshot declares com.example.Zed, which nothing reaches")
        fails(
            Bytes.snapshot(Bytes.type("com.example.User"), Bytes.record("com.example.User", "x" to Bytes.of { writeByte(2); string("T"); writeByte(0) })),
            "com.example.User refers to a type parameter T it doesn't declare",
        )
        fails(Bytes.snapshot(Bytes.type("kotlin.collections.List")), "kotlin.collections.List has 0 type arguments, and kotlin.collections.List takes 1")
        fails(Bytes.snapshot(Bytes.of { writeByte(1); string("kotlin.collections.List"); writeByte(0); writeInt(1); writeByte(0) }), "a star projection")
        fails(Bytes.snapshot(Bytes.type("com.example.User"), Bytes.record("com.example.User", "id" to Bytes.type("kotlin.Long"), "id" to Bytes.type("kotlin.Int"))), "two fields named id")
        fails(
            Bytes.snapshot(Bytes.type("com.example.Color"), Bytes.of { writeByte(3); string("com.example.Color"); writeInt(2); writeInt(2); string("B"); writeInt(1); string("A"); writeInt(0) }),
            "constant ID 1 follows 2",
        )
        fails(
            Bytes.snapshot(Bytes.type("com.example.Color"), Bytes.of { writeByte(3); string("com.example.Color"); writeInt(1); writeInt(0); string("A"); writeInt(0) }),
            "constant ID 0 follows 0: IDs are positive and ascending",
        )
        fails(Bytes.snapshot(Bytes.type("")), "a type name is empty")
        fails(Bytes.snapshot(Bytes.type("x".repeat(65_536))), "a type name is 65536 chars long, over the budget of 65535")
        fails(Bytes.of { writeInt(1); writeByte(0); write(Bytes.type("kotlin.Long")); writeInt(-1) }, "declarations: a count of -1")
        fails(Bytes.of { writeInt(1); writeByte(0); write(Bytes.type("kotlin.Long")); writeInt(Int.MAX_VALUE) }, "a count of 2147483647 can't fit")
    }

    @Test
    fun `an unknown critical attribute fails, and an unknown other one is skipped`() {
        fun userWithFieldAttribute(tag: Int, critical: Boolean) = Bytes.snapshot(
            Bytes.type("com.example.User"),
            Bytes.of {
                writeByte(1); string("com.example.User"); writeInt(0); writeInt(1)
                string("id"); write(Bytes.type("kotlin.Long"))
                writeInt(1); writeInt(tag); writeByte(if (critical) 1 else 0); writeInt(3); write(byteArrayOf(1, 2, 3))
                writeInt(0)
            },
        )
        val skipped = PersistedSchema.read(DataInputDeserializer(userWithFieldAttribute(5, critical = false)))
        assertEquals(RecordSchema("com.example.User", listOf(field("id", "kotlin.Long"))), skipped.declaration("com.example.User"))

        val e = assertThrows<MalformedDataException> {
            PersistedSchema.read(DataInputDeserializer(userWithFieldAttribute(PersistedSchema.DROPPED_VALUE_ATTRIBUTE, critical = true)))
        }
        assertEquals("com.example.User.id has critical attribute 1, which this reader of snapshot format version 1 doesn't know", e.message)

        val unordered = Bytes.of {
            writeInt(1); writeByte(0); write(Bytes.type("kotlin.Long")); writeInt(0)
            writeInt(2); writeInt(4); writeByte(0); writeInt(0); writeInt(4); writeByte(0); writeInt(0)
        }
        val order = assertThrows<MalformedDataException> { PersistedSchema.read(DataInputDeserializer(unordered)) }
        assertEquals("the snapshot: attribute 4 follows 4: tags are positive and ascending", order.message)

        val tooLong = Bytes.of {
            writeInt(1); writeByte(0); write(Bytes.type("kotlin.Long")); writeInt(0)
            writeInt(1); writeInt(4); writeByte(0); writeInt(PersistedSchema.MAX_SNAPSHOT_BYTES)
        }
        val budget = assertThrows<MalformedDataException> { PersistedSchema.read(DataInputDeserializer(tooLong)) }
        assertTrue("can't fit in the" in budget.message.orEmpty(), budget.message)
    }

    @Test
    fun `nesting is bounded, by the writer and the reader alike`() {
        fun lists(depth: Int): SchemaType.ClassType =
            (1..depth).fold(type("kotlin.Long")) { inner, _ -> SchemaType.ClassType("kotlin.collections.List", listOf(SchemaTypeArgument.invariant(inner))) }

        // 63 lists around a Long: 64 levels, the budget.
        val deepest = PersistedSchema.of(lists(63)) { null }
        assertEquals(lists(63), PersistedSchema.read(DataInputDeserializer(deepest.encoded)).root)

        assertThrows<IllegalArgumentException> { PersistedSchema.of(lists(64)) { null } }
        val tooDeep = Bytes.of {
            writeInt(1); writeByte(0)
            repeat(64) { writeByte(1); string("kotlin.collections.List"); writeByte(0); writeInt(1); writeByte(1) }
            write(Bytes.type("kotlin.Long"))
            writeInt(0); writeInt(0)
        }
        val e = assertThrows<MalformedDataException> { PersistedSchema.read(DataInputDeserializer(tooDeep)) }
        assertEquals("a type nests deeper than 64 levels", e.message)

        // Nesting through records counts too: R1(next: R2), ..., R65(value: Long) is 66 levels.
        val chain = (1..65).associate { i ->
            val name = "com.example.R%02d".format(i)
            name to RecordSchema(name, listOf(if (i < 65) field("next", "com.example.R%02d".format(i + 1)) else field("value", "kotlin.Long")))
        }
        val writer = assertThrows<IllegalArgumentException> { PersistedSchema.of(type("com.example.R01")) { chain[it] } }
        assertEquals("com.example.R01 nests deeper than 64 levels, over the budget", writer.message)
        val chainBytes = Bytes.snapshot(
            Bytes.type("com.example.R01"),
            *chain.values.map { record -> Bytes.record(record.typeName, *record.fields.map { it.name to Bytes.type((it.type as SchemaType.ClassType).name) }.toTypedArray()) }.toTypedArray(),
        )
        val reader = assertThrows<MalformedDataException> { PersistedSchema.read(DataInputDeserializer(chainBytes)) }
        assertEquals("the snapshot holds an invalid schema: com.example.R01 nests deeper than 64 levels, over the budget", reader.message)

        // R01(next: R02), ..., R63(value: Long) is 64 levels, the budget.
        val longest = chain.filterKeys { it <= "com.example.R63" } +
            ("com.example.R63" to RecordSchema("com.example.R63", listOf(field("value", "kotlin.Long"))))
        val schema = PersistedSchema.of(type("com.example.R01")) { longest[it] }
        assertEquals(schema.declarations, PersistedSchema.read(DataInputDeserializer(schema.encoded)).declarations)
    }

    @Test
    fun `a chain of declarations far deeper than the budget fails with the diagnostic, on a small stack`() {
        // R000001(next: R000002), ..., R100000(value: Long): about 5 MB, within the size budget.
        val length = 100_000
        fun name(i: Int) = "com.example.R%06d".format(i)
        val chain = (1..length).associate { i ->
            name(i) to RecordSchema(name(i), listOf(if (i < length) field("next", name(i + 1)) else field("value", "kotlin.Long")))
        }
        fun snapshot(last: Pair<String, ByteArray>) = Bytes.snapshot(
            Bytes.type(name(1)),
            *(1 until length).map { Bytes.record(name(it), "next" to Bytes.type(name(it + 1))) }.toTypedArray(),
            Bytes.record(name(length), last),
        )
        val chainBytes = snapshot("value" to Bytes.type("kotlin.Long"))
        // The same chain closed into a cycle: R100000(next: R000001).
        val cycleBytes = snapshot("next" to Bytes.type(name(1)))
        assertTrue(chainBytes.size < PersistedSchema.MAX_SNAPSHOT_BYTES, "${chainBytes.size} bytes")

        // A checker that recursed through the chain would overflow this stack long before its end.
        onSmallStack {
            for (bytes in listOf(chainBytes, cycleBytes)) {
                val reader = assertThrows<MalformedDataException> { PersistedSchema.read(DataInputDeserializer(bytes)) }
                assertEquals("the snapshot holds an invalid schema: com.example.R000001 nests deeper than 64 levels, over the budget", reader.message)
            }
            val writer = assertThrows<IllegalArgumentException> { PersistedSchema.of(type(name(1))) { chain[it] } }
            assertEquals("com.example.R000001 nests deeper than 64 levels, over the budget", writer.message)
        }
    }

    @Test
    fun `a generic schema's nesting is computed once per declaration, not once per instantiation`() {
        // D01<T>(a: D02<A<T>>, b: D02<B<T>>, c: D02<C<T>>), ..., Dn<T>(value: T), and A, B and C<T>(value: T). A
        // D01<Long> nests 2n levels, through 3^(n-1) instantiations of Dn: one per word of A, B and C.
        fun d(i: Int) = "com.example.D%02d".format(i)
        val wrappers = listOf("com.example.A", "com.example.B", "com.example.C")
        val t = SchemaType.TypeParameter("T")
        fun generic(name: String, argument: SchemaType) = SchemaType.ClassType(name, listOf(SchemaTypeArgument.invariant(argument)))
        fun declarations(n: Int): Map<String, DeclarationSchema> = (1..n).associate { i ->
            val fields = if (i < n) {
                wrappers.map { FieldSchema(it.substringAfterLast('.').lowercase(), generic(d(i + 1), generic(it, t))) }
            } else {
                listOf(FieldSchema("value", t))
            }
            d(i) to RecordSchema(d(i), fields, listOf("T"))
        } + wrappers.associateWith { RecordSchema(it, listOf(FieldSchema("value", t)), listOf("T")) }
        val root = generic(d(1), type("kotlin.Long"))

        assertTimeoutPreemptively(Duration.ofSeconds(60)) {
            // n = 32 nests 64 levels, the budget.
            val longest = declarations(32)
            val deepest = PersistedSchema.of(root) { longest[it] }
            assertEquals(deepest.declarations, PersistedSchema.read(DataInputDeserializer(deepest.encoded)).declarations)
            val tooLong = declarations(33)
            val e = assertThrows<IllegalArgumentException> { PersistedSchema.of(root) { tooLong[it] } }
            assertEquals("com.example.D01<kotlin.Long> nests deeper than 64 levels, over the budget", e.message)
        }
    }

    @Test
    fun `truncated data fails as the end of the input`() {
        val schema = PersistedSchema.of(type("com.example.User")) { mapOf("com.example.User" to userSchema)[it] }
        for (length in 0 until schema.encoded.size) {
            assertThrows<EOFException>("$length bytes") { PersistedSchema.read(DataInputDeserializer(schema.encoded.copyOf(length))) }
        }
    }

    private val colors = listOf(EnumConstantSchema("BLUE", 7), EnumConstantSchema("RED", 1), EnumConstantSchema("GREEN", 2))

    private val userCreatedWithLong = mapOf(
        "com.example.UserCreated" to RecordSchema("com.example.UserCreated", listOf(field("userId", "kotlin.Long"), field("color", "com.example.Color"))),
    )

    /** Every planned shape: a generic record, collections, nullability at several levels, a value class, an enum, nested sealed types and an object. */
    private val everyShape: Map<String, DeclarationSchema> = mapOf(
        "com.example.Envelope" to RecordSchema(
            "com.example.Envelope",
            listOf(
                FieldSchema("payload", SchemaType.TypeParameter("T")),
                FieldSchema(
                    "meta",
                    SchemaType.ClassType(
                        "kotlin.collections.Map",
                        listOf(
                            SchemaTypeArgument.invariant(type("kotlin.String")),
                            SchemaTypeArgument.invariant(
                                SchemaType.ClassType("kotlin.collections.List", listOf(SchemaTypeArgument.invariant(type("com.example.Event", nullable = true)))),
                            ),
                        ),
                    ),
                ),
                FieldSchema("previous", SchemaType.TypeParameter("T", isNullable = true)),
            ),
            listOf("T"),
        ),
        "com.example.User" to userSchema,
        "com.example.UserId" to ValueClassSchema("com.example.UserId", field("value", "kotlin.Long")),
        "com.example.Color" to EnumSchema("com.example.Color", colors),
        "com.example.Event" to SealedSchema(
            "com.example.Event",
            listOf(SealedSubtypeSchema(2, type("com.example.Shutdown")), SealedSubtypeSchema(1, type("com.example.UserEvent"))),
        ),
        "com.example.UserEvent" to SealedSchema("com.example.UserEvent", listOf(SealedSubtypeSchema(1, type("com.example.UserCreated")))),
        "com.example.UserCreated" to RecordSchema("com.example.UserCreated", listOf(field("userId", "com.example.UserId"), field("color", "com.example.Color"))),
        "com.example.Shutdown" to ObjectSchema("com.example.Shutdown"),
    )

    private val envelopeOfUser = SchemaType.ClassType("com.example.Envelope", listOf(SchemaTypeArgument.invariant(type("com.example.User"))))

    private companion object {
        fun type(name: String, nullable: Boolean = false) = SchemaType.ClassType(name, isNullable = nullable)

        fun field(name: String, type: String) = FieldSchema(name, type(type))

        /** The decision's example: `User(id: Long, name: String)`'s snapshot data, in the value role. */
        val USER_SNAPSHOT = """
            00 00 00 01                         record format version 1
            00                                  role: value
            01 11 63 6f 6d 2e 65 78 61 6d 70 6c 65 2e 55 73 65 72 00 00 00 00 00
                                                root: class type com.example.User, not nullable, no arguments
            00 00 00 01                         one declaration
            01 11 63 6f 6d 2e 65 78 61 6d 70 6c 65 2e 55 73 65 72
                                                a record, com.example.User
            00 00 00 00                         no type parameters
            00 00 00 02                         two fields, in constructor order
            03 69 64                            id
            01 0c 6b 6f 74 6c 69 6e 2e 4c 6f 6e 67 00 00 00 00 00
                                                kotlin.Long
            00 00 00 00                         no field attributes
            05 6e 61 6d 65                      name
            01 0e 6b 6f 74 6c 69 6e 2e 53 74 72 69 6e 67 00 00 00 00 00
                                                kotlin.String
            00 00 00 00                         no field attributes
            00 00 00 00                         no declaration attributes
            00 00 00 00                         no snapshot attributes
        """.trimIndent()
    }
}

internal fun DataOutputStream.string(value: String) = RecordFormat.writeString(this, value)

/** Runs [block] on a thread with a 256 KiB stack, and rethrows what it throws. */
private fun onSmallStack(block: () -> Unit) {
    var failure: Throwable? = null
    val thread = Thread(null, { failure = runCatching(block).exceptionOrNull() }, "small stack", 256L * 1024)
    thread.start()
    thread.join()
    failure?.let { throw it }
}

/** Snapshot data written by hand, for the cases [PersistedSchema.of] refuses to write. */
internal object Bytes {
    fun of(write: DataOutputStream.() -> Unit): ByteArray = ByteArrayOutputStream().also { DataOutputStream(it).write() }.toByteArray()

    fun type(name: String): ByteArray = of { writeByte(1); string(name); writeByte(0); writeInt(0) }

    fun record(name: String, vararg fields: Pair<String, ByteArray>): ByteArray = of {
        writeByte(1); string(name); writeInt(0); writeInt(fields.size)
        for ((field, type) in fields) {
            string(field); write(type); writeInt(0)
        }
        writeInt(0)
    }

    fun snapshot(root: ByteArray, vararg declarations: ByteArray): ByteArray = snapshotWithRole(0, root, *declarations)

    fun snapshotWithRole(role: Int, root: ByteArray, vararg declarations: ByteArray): ByteArray = of {
        writeInt(1); writeByte(role); write(root); writeInt(declarations.size)
        declarations.forEach { write(it) }
        writeInt(0)
    }
}
