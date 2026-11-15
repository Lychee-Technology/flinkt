package io.flinkt.flink.format

import io.flinkt.codec.GeneratedCodec
import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.schema.SchemaType
import java.io.IOException
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.DataInputDeserializer
import org.apache.flink.core.memory.DataOutputSerializer
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows

/**
 * Prototype restores (docs/persisted-format.md, "Restore"), as #5's evidence: state bytes and a snapshot written by
 * an old declaration, and a restore through Flink's own `TypeSerializerSnapshot.readVersionedSnapshot`, which
 * creates the snapshot class by name through a class loader that holds only the current declaration.
 *
 * The snapshot, its schema and the planner are prototypes of #12's, #34's and #35's. [PrototypeSerializerSnapshot]'s
 * `restoreSerializer()` is the interim policy #12 ships, as-is or fail; `restore(enabled)` switches on a migration
 * the way its stage will, to show that the format can execute it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RestorePrototypeTest {
    private val user by lazy { Declarations.load(Declarations.USER) }

    @Test
    fun `an identical schema restores with the current codec, which reads the old bytes`() {
        val old = OldState.write(Declarations.load(Declarations.USER), "com.example.User")
        val snapshot = old.readSnapshot(user)

        val restored = snapshot.restoreSerializer()
        assertInstanceOf(PrototypeSerializer::class.java, restored)
        assertEquals(old.values, old.readAll(restored))
        assertTrue(currentSnapshot(user, "com.example.User").resolveSchemaCompatibility(snapshot).isCompatibleAsIs)
    }

    @Test
    fun `User(id) restored as User(id, name) fails, and never returns a serializer that misreads the old bytes`() {
        val old = OldState.write(Declarations.load(Declarations.USER_ID_ONLY), "com.example.User")
        assertEquals(listOf("User(id=1)", "User(id=306636617412509696)"), old.values)

        // Records carry no schema, so the current codec reads two old records as one value without noticing.
        val current = codec(user, "com.example.User")
        assertEquals("User(id=1, name=Ada)", current.deserialize(DataInputDeserializer(old.bytes)).toString())

        val snapshot = old.readSnapshot(user)
        for (enabled in listOf(emptySet(), Operation.entries.toSet())) {
            val e = assertThrows<IncompatibleStateException> { snapshot.restore(enabled) }
            assertEquals(
                "Can't restore com.example.User (role: value): com.example.User.name was added as kotlin.String, " +
                    "without a migration default (#37). Old: record com.example.User(id: kotlin.Long). " +
                    "Current: record com.example.User(id: kotlin.Long, name: kotlin.String)",
                e.message,
            )
        }
        assertThrows<IncompatibleStateException> { snapshot.restoreSerializer() }
        assertTrue(currentSnapshot(user, "com.example.User").resolveSchemaCompatibility(snapshot).isIncompatible)
    }

    @Test
    fun `a removed field whose class is gone is consumed by walking its old schema`() {
        val old = OldState.write(Declarations.load(Declarations.USER_WITH_PROFILE), "com.example.User")
        for (gone in listOf("com.example.OldProfile", "com.example.Address")) {
            assertThrows<ClassNotFoundException> { Class.forName(gone, false, user) }
        }
        val snapshot = old.readSnapshot(user)

        // Until stage 4 (#36), removing a field is incompatible, and says so.
        val interim = assertThrows<IncompatibleStateException> { snapshot.restoreSerializer() }
        assertTrue("com.example.User.profile was removed. Removing a field migrates from stage 4, #36" in interim.message.orEmpty(), interim.message)
        assertTrue(currentSnapshot(user, "com.example.User").resolveSchemaCompatibility(snapshot).isIncompatible)

        // With removal enabled, the old-layout serializer reads every old record into a current User, and consumes
        // each profile, its nullable nested Address included, from the schema in the snapshot.
        val restored = snapshot.restore(setOf(Operation.REMOVE_FIELD))
        assertInstanceOf(OldLayoutSerializer::class.java, restored)
        assertEquals(listOf("User(id=1, name=Ada)", "User(id=2, name=Grace)", "User(id=3, name=Barbara)"), old.readAll(restored))
        // It reports the old schema as its snapshot, byte for byte.
        assertArrayEquals(snapshot.persistedSchema.encoded, (restored.snapshotConfiguration() as PrototypeSerializerSnapshot<*>).persistedSchema.encoded)
    }

    @Test
    fun `a malformed removed value fails the read instead of shifting the next record`() {
        val old = OldState.write(Declarations.load(Declarations.USER_WITH_PROFILE), "com.example.User")
        // The first record's profile bitmap is the byte after its id (8 bytes) and name (04 41 64 61).
        val corrupt = old.bytes.copyOf().also { it[12] = 0x04 }
        val restored = old.readSnapshot(user).restore(setOf(Operation.REMOVE_FIELD))
        val e = assertThrows<MalformedDataException> { restored.deserialize(DataInputDeserializer(corrupt)) }
        assertEquals("a null bitmap for 2 nullable fields has bit 2 set: 04 in its last byte", e.message)
    }

    @Test
    fun `a removed map that holds a key twice fails the read, as its codec does`() {
        val oldLoader = Declarations.load(Declarations.USER_WITH_TAGS)
        val old = OldState.write(oldLoader, "com.example.User")
        val restored = old.readSnapshot(user).restore(setOf(Operation.REMOVE_FIELD))
        assertEquals(listOf("User(id=1, name=Ada)", "User(id=2, name=Grace)"), old.readAll(restored))

        // The first record's second key, "b", made "a". Before it: the id (8 bytes), the name (04 41 64 61), the
        // map's size (4 bytes), the key "a" (02 61) and its Long (8 bytes), then the key's length.
        val corrupt = old.bytes.copyOf().also {
            assertEquals(0x62, it[27].toInt())
            it[27] = 0x61
        }
        val codec = assertThrows<MalformedDataException> { codec(oldLoader, "com.example.User").deserialize(DataInputDeserializer(corrupt)) }
        assertEquals("a map holds the key a twice", codec.message)
        val e = assertThrows<MalformedDataException> { restored.deserialize(DataInputDeserializer(corrupt)) }
        assertEquals("a map holds the key written as 02 61 twice", e.message)
    }

    @Test
    fun `without PersistedFieldOrder, a constructor reorder reorders the layout, and fails in every role`() {
        val oldLoader = Declarations.load(Declarations.KEY_AB)
        val reordered = Declarations.load(Declarations.KEY_BA)
        for (role in KeyRole.entries) {
            val old = OldState.write(oldLoader, "com.example.Key", role)
            val e = assertThrows<IncompatibleStateException> { old.readSnapshot(reordered).restore(Operation.entries.toSet()) }
            assertEquals(
                "Can't restore com.example.Key (role: ${role.name.lowercase().replace('_', ' ')}): the persisted order of " +
                    "com.example.Key changed from (a, b) to (b, a). Its records are written in that order, and reading " +
                    "them in another is a field permutation, which no migration stage performs. Without " +
                    "@PersistedFieldOrder, the persisted order is the constructor order: to reorder the constructor and " +
                    "keep the layout, give each property a @PersistedFieldOrder in the old order",
                e.message,
            )
        }
        val value = OldState.write(oldLoader, "com.example.Key")
        assertTrue(currentSnapshot(reordered, "com.example.Key").resolveSchemaCompatibility(value.readSnapshot(reordered)).isIncompatible)
    }

    @Test
    fun `with PersistedFieldOrder keeping the layout, a constructor reorder restores as-is in a value and a map key, and fails in a partition key`() {
        // Key(a, b), written in constructor order, restored as Key(@PersistedFieldOrder(7) b, @PersistedFieldOrder(3) a).
        val oldLoader = Declarations.load(Declarations.KEY_AB)
        val reordered = Declarations.load(Declarations.KEY_BA_PINNED)

        for (role in listOf(KeyRole.VALUE, KeyRole.MAP_KEY)) {
            val old = OldState.write(oldLoader, "com.example.Key", role)
            val restored = old.readSnapshot(reordered).restoreSerializer()
            assertEquals(listOf("Key(b=2, a=1)"), old.readAll(restored))
        }
        val value = OldState.write(oldLoader, "com.example.Key")
        assertTrue(currentSnapshot(reordered, "com.example.Key").resolveSchemaCompatibility(value.readSnapshot(reordered)).isCompatibleAsIs)

        val partitionKey = OldState.write(oldLoader, "com.example.Key", KeyRole.PARTITION_KEY)
        val e = assertThrows<IncompatibleStateException> { partitionKey.readSnapshot(reordered).restoreSerializer() }
        assertEquals(
            "Can't restore com.example.Key (role: partition key): the constructor order of com.example.Key changed " +
                "from (a, b) to (b, a). Its bytes didn't change, but a data class's generated hashCode() follows " +
                "constructor order, so the partition key's key groups would move",
            e.message,
        )
    }

    @Test
    fun `a constructor reorder of a record under a Map key fails the restore, instead of a read the old writer's map would fail`() {
        // Holder(keys: Map<Key, Boolean>) with 1,025 keys Key(-31 * i, i): hash codes -960 * i, all different, so
        // the old release wrote the map. Key's constructor is then reordered, keeping its persisted order.
        val oldLoader = Declarations.load(Declarations.HOLDER_AB_PINNED)
        val reordered = Declarations.load(Declarations.HOLDER_BA_PINNED)
        val old = OldState.write(oldLoader, "com.example.Holder")
        assertEquals(9_229, old.bytes.size)

        // Every key now hashes to 31 * i - 31 * i = 0. Read as-is, the current codec would fail at the 1,025th key.
        val read = assertThrows<MalformedDataException> { codec(reordered, "com.example.Holder").deserialize(DataInputDeserializer(old.bytes)) }
        assertEquals(
            "a map holds more than 1024 keys whose hashCode() is 0, such as Key(b=1024, a=-31744), over the budget of " +
                "1024 keys per hash code",
            read.message,
        )

        // So the plan isn't as-is: the restore fails with the planner's diagnostic before anything is read.
        val snapshot = old.readSnapshot(reordered)
        for (enabled in listOf(emptySet(), Operation.entries.toSet())) {
            val e = assertThrows<IncompatibleStateException> { snapshot.restore(enabled) }
            assertEquals(
                "Can't restore com.example.Holder (role: value): the constructor order of com.example.Key changed from " +
                    "(a, b) to (b, a). Its bytes didn't change, but a data class's generated hashCode() follows " +
                    "constructor order, and com.example.Key is in a Map key, whose keys are counted by hash code where " +
                    "the map is written and where it's read. A map written within the budget of 1024 keys per hash " +
                    "code could be over it when it's read",
                e.message,
            )
        }
        assertTrue(currentSnapshot(reordered, "com.example.Holder").resolveSchemaCompatibility(snapshot).isIncompatible)

        // A change that is as-is keeps every key's hash code, so the same map restores and reads: here the keys are
        // renumbered, which changes no order, and so no byte of the snapshot.
        val renumbered = Declarations.load(Declarations.HOLDER_AB_RENUMBERED)
        val restored = old.readSnapshot(renumbered).restoreSerializer()
        assertInstanceOf(PrototypeSerializer::class.java, restored)
        assertEquals(old.values, old.readAll(restored))
        assertArrayEquals(
            (currentSnapshot(oldLoader, "com.example.Holder") as PrototypeSerializerSnapshot<*>).persistedSchema.encoded,
            (currentSnapshot(renumbered, "com.example.Holder") as PrototypeSerializerSnapshot<*>).persistedSchema.encoded,
        )
    }

    @Test
    fun `a map key never migrates, even with removal enabled`() {
        val old = OldState.write(Declarations.load(Declarations.USER_WITH_PROFILE), "com.example.User", KeyRole.MAP_KEY)
        val e = assertThrows<IncompatibleStateException> { old.readSnapshot(user).restore(setOf(Operation.REMOVE_FIELD)) }
        assertTrue(e.message.orEmpty().startsWith("Can't restore com.example.User (role: map key): a key's schema doesn't change"), e.message)
    }

    @Test
    fun `a state whose type the job no longer has fails the restore`() {
        val old = OldState.write(Declarations.load(Declarations.USER), "com.example.User")
        val e = assertThrows<IncompatibleStateException> { old.readSnapshot(Declarations.load(Declarations.KEY_AB)).restoreSerializer() }
        assertTrue(e.message.orEmpty().startsWith("Can't restore com.example.User: the job has no class com.example.User"), e.message)
    }

    @Test
    fun `an unknown snapshot format version or record format version fails explicitly`() {
        val old = OldState.write(Declarations.load(Declarations.USER), "com.example.User")
        // Flink's framing: the snapshot class name (writeUTF), the snapshot format version, then the data, whose
        // first field is the record format version.
        val nameLength = 2 + PrototypeSerializerSnapshot::class.java.name.length
        val version = old.snapshot.copyOf().also { it[nameLength + 3] = 2 }
        val e = assertThrows<IOException> {
            TypeSerializerSnapshot.readVersionedSnapshot<Any>(DataInputDeserializer(version), user)
        }
        assertTrue("written in Flinkt snapshot format version 2, and this release reads version 1" in e.message.orEmpty(), e.message)

        val recordVersion = old.snapshot.copyOf().also { it[nameLength + 7] = 2 }
        val snapshot = TypeSerializerSnapshot.readVersionedSnapshot<Any>(DataInputDeserializer(recordVersion), user)
            as PrototypeSerializerSnapshot<Any>
        val restore = assertThrows<IncompatibleStateException> { snapshot.restoreSerializer() }
        assertEquals(
            "Can't restore com.example.User (role: value): its records were written in record format version 2, " +
                "and this release reads version 1",
            restore.message,
        )
    }

    /** State an old declaration wrote: its values' bytes, back to back, and its snapshot as Flink writes it. */
    private class OldState(val bytes: ByteArray, val snapshot: ByteArray, val values: List<String>) {
        fun readSnapshot(loader: ClassLoader): PrototypeSerializerSnapshot<Any> =
            TypeSerializerSnapshot.readVersionedSnapshot<Any>(DataInputDeserializer(snapshot), loader)
                as PrototypeSerializerSnapshot<Any>

        /** Reads every value, and checks that nothing is left. */
        fun readAll(serializer: TypeSerializer<Any>): List<String> {
            val input = DataInputDeserializer(bytes)
            val read = values.map { serializer.deserialize(input).toString() }
            assertEquals(0, input.available(), "bytes left after reading ${values.size} values")
            return read
        }

        companion object {
            fun write(loader: ClassLoader, root: String, role: KeyRole = KeyRole.VALUE): OldState {
                val serializer = serializer(loader, root, role)
                val values = Declarations.samples(loader)
                val data = DataOutputSerializer(64)
                values.forEach { serializer.serialize(it, data) }
                val snapshot = DataOutputSerializer(64)
                TypeSerializerSnapshot.writeVersionedSnapshot(snapshot, serializer.snapshotConfiguration())
                return OldState(data.copyOfBuffer, snapshot.copyOfBuffer, values.map { it.toString() })
            }
        }
    }

    private fun currentSnapshot(loader: ClassLoader, root: String): TypeSerializerSnapshot<Any> =
        serializer(loader, root, KeyRole.VALUE).snapshotConfiguration()

    private fun codec(loader: ClassLoader, root: String): GeneratedCodec<Any> = registryCodec(loader, root)
}

/** The serializer a job builds for [root] through [loader]'s registry, as an adapter's `TypeInformation` would. */
private fun serializer(loader: ClassLoader, root: String, role: KeyRole): PrototypeSerializer<Any> {
    val registry = TypeRegistry.load(loader)
    val schema = PersistedSchema.of(SchemaType.ClassType(root), role) { name ->
        registry.declaration(Class.forName(name, false, loader))?.schema
    }
    return PrototypeSerializer(registryCodec(loader, root), schema)
}

@Suppress("UNCHECKED_CAST")
private fun registryCodec(loader: ClassLoader, root: String): GeneratedCodec<Any> =
    (TypeRegistry.load(loader).resolve(kotlin.jvm.internal.Reflection.typeOf(loader.loadClass(root))) as Resolution.Codec)
        .codec as GeneratedCodec<Any>
