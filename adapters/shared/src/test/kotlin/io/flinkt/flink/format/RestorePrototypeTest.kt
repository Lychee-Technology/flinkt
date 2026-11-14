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
    fun `a constructor reorder restores as-is in a value, and fails in a partition key`() {
        val oldLoader = Declarations.load(Declarations.KEY_AB)
        val reordered = Declarations.load(Declarations.KEY_BA)

        val value = OldState.write(oldLoader, "com.example.Key", KeyRole.VALUE)
        val restored = value.readSnapshot(reordered).restoreSerializer()
        assertEquals(listOf("Key(b=2, a=1)"), value.readAll(restored))

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
