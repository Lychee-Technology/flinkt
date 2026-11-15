package io.flinkt.flink.format

import io.flinkt.codec.GeneratedCodec
import java.io.DataInput
import java.io.DataOutput

/*
 * The decision's byte-level examples (docs/persisted-format.md, "Examples"), with codecs written by hand the way KSP
 * will generate them (#11): fields in persisted order, which is constructor order unless the properties carry
 * @PersistedFieldOrder, each read into a local, then one call of the primary constructor, by name. #12 and #15 turn
 * the examples into golden fixtures.
 */

/**
 * A stand-in for the approved `@PersistedFieldOrder` (docs/persisted-format.md, "Field order"), which #11 adds to
 * `flinkt-core` with KSP's checks of it. Here it shows the order a hand-written codec follows. flinkt-ksp's
 * `PersistedFieldOrderTest` tests the checks, with a prototype processor.
 */
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.BINARY)
annotation class PersistedFieldOrder(val order: Int)

data class User(val id: Long, val name: String)

/** Persisted order: `id`, `name`, the constructor's. No nullable field, so no bitmap. */
object UserCodec : GeneratedCodec<User> {
    override fun serialize(value: User, output: DataOutput) {
        RecordFormat.writeLong(output, value.id)
        RecordFormat.writeString(output, value.name)
    }

    override fun deserialize(input: DataInput): User {
        val id = RecordFormat.readLong(input)
        val name = RecordFormat.readString(input)
        return User(id = id, name = name)
    }

    override fun copy(value: User): User = value
}

/** The nullable variant. */
data class Account(
    val id: Long,
    val name: String?,
    val email: String?,
    val balance: Double?,
    val rating: Float,
)

/**
 * Persisted order: `id`, `name`, `email`, `balance`, `rating`, the constructor's. The nullable fields in that order
 * are `name`, `email` and `balance`: bits 0, 1 and 2 of a one-byte bitmap, which comes first.
 */
object AccountCodec : GeneratedCodec<Account> {
    override fun serialize(value: Account, output: DataOutput) {
        val name = value.name
        val email = value.email
        val balance = value.balance
        var nulls = 0
        if (name == null) nulls = nulls or 0x01
        if (email == null) nulls = nulls or 0x02
        if (balance == null) nulls = nulls or 0x04
        output.writeByte(nulls)
        RecordFormat.writeLong(output, value.id)
        if (name != null) RecordFormat.writeString(output, name)
        if (email != null) RecordFormat.writeString(output, email)
        if (balance != null) RecordFormat.writeDouble(output, balance)
        RecordFormat.writeFloat(output, value.rating)
    }

    override fun deserialize(input: DataInput): Account {
        val nulls = input.readUnsignedByte()
        if (nulls and 0xf8 != 0) throw MalformedDataException("Account's null bitmap has 3 bits, not ${"%02x".format(nulls)}")
        val id = RecordFormat.readLong(input)
        val name = if (nulls and 0x01 != 0) null else RecordFormat.readString(input)
        val email = if (nulls and 0x02 != 0) null else RecordFormat.readString(input)
        val balance = if (nulls and 0x04 != 0) null else RecordFormat.readDouble(input)
        val rating = RecordFormat.readFloat(input)
        return Account(id = id, name = name, email = email, balance = balance, rating = rating)
    }

    override fun copy(value: Account): Account = value
}

/**
 * [Account] with `@PersistedFieldOrder` on every property: sparse keys, which give it a persisted order other than
 * its constructor's. Only their order counts, so renumbering them without changing it changes no byte.
 */
data class PinnedAccount(
    @property:PersistedFieldOrder(order = 30) val id: Long,
    @property:PersistedFieldOrder(order = 10) val name: String?,
    @property:PersistedFieldOrder(order = 20) val email: String?,
    @property:PersistedFieldOrder(order = 99) val balance: Double?,
    @property:PersistedFieldOrder(order = 0) val rating: Float,
)

/**
 * Persisted order: `rating` (0), `name` (10), `email` (20), `id` (30), `balance` (99). The bitmap follows it too:
 * `name` is bit 0, `email` bit 1 and `balance` bit 2.
 */
object PinnedAccountCodec : GeneratedCodec<PinnedAccount> {
    override fun serialize(value: PinnedAccount, output: DataOutput) {
        val name = value.name
        val email = value.email
        val balance = value.balance
        var nulls = 0
        if (name == null) nulls = nulls or 0x01
        if (email == null) nulls = nulls or 0x02
        if (balance == null) nulls = nulls or 0x04
        output.writeByte(nulls)
        RecordFormat.writeFloat(output, value.rating)
        if (name != null) RecordFormat.writeString(output, name)
        if (email != null) RecordFormat.writeString(output, email)
        RecordFormat.writeLong(output, value.id)
        if (balance != null) RecordFormat.writeDouble(output, balance)
    }

    override fun deserialize(input: DataInput): PinnedAccount {
        val nulls = input.readUnsignedByte()
        if (nulls and 0xf8 != 0) throw MalformedDataException("PinnedAccount's null bitmap has 3 bits, not ${"%02x".format(nulls)}")
        val rating = RecordFormat.readFloat(input)
        val name = if (nulls and 0x01 != 0) null else RecordFormat.readString(input)
        val email = if (nulls and 0x02 != 0) null else RecordFormat.readString(input)
        val id = RecordFormat.readLong(input)
        val balance = if (nulls and 0x04 != 0) null else RecordFormat.readDouble(input)
        return PinnedAccount(id = id, name = name, email = email, balance = balance, rating = rating)
    }

    override fun copy(value: PinnedAccount): PinnedAccount = value
}

/** A record with nested records, one of them nullable. */
data class Session(val user: User, val note: String?, val previous: User?)

/** Persisted order: `user`, `note`, `previous`. Nested records are written inline, with no length or header. */
object SessionCodec : GeneratedCodec<Session> {
    override fun serialize(value: Session, output: DataOutput) {
        val note = value.note
        val previous = value.previous
        var nulls = 0
        if (note == null) nulls = nulls or 0x01
        if (previous == null) nulls = nulls or 0x02
        output.writeByte(nulls)
        UserCodec.serialize(value.user, output)
        if (note != null) RecordFormat.writeString(output, note)
        if (previous != null) UserCodec.serialize(previous, output)
    }

    override fun deserialize(input: DataInput): Session {
        val nulls = input.readUnsignedByte()
        if (nulls and 0xfc != 0) throw MalformedDataException("Session's null bitmap has 2 bits, not ${"%02x".format(nulls)}")
        val user = UserCodec.deserialize(input)
        val note = if (nulls and 0x01 != 0) null else RecordFormat.readString(input)
        val previous = if (nulls and 0x02 != 0) null else UserCodec.deserialize(input)
        return Session(user = user, note = note, previous = previous)
    }

    override fun copy(value: Session): Session = value
}

/** Nine nullable fields: a two-byte bitmap, whose bits follow the persisted order, here the constructor's. */
data class Wide(
    val i: Boolean?,
    val h: Boolean?,
    val g: Boolean?,
    val f: Boolean?,
    val e: Boolean?,
    val d: Boolean?,
    val c: Boolean?,
    val b: Boolean?,
    val a: Boolean?,
)

/** Persisted order `i` to `a`: bits 0 to 7 of the first bitmap byte, then bit 0 of the second. */
object WideCodec : GeneratedCodec<Wide> {
    private fun Wide.fields(): List<Boolean?> = listOf(i, h, g, f, e, d, c, b, a)

    override fun serialize(value: Wide, output: DataOutput) {
        val fields = value.fields()
        RecordFormat.writeNullBitmap(output, BooleanArray(fields.size) { fields[it] == null })
        for (field in fields) if (field != null) RecordFormat.writeBoolean(output, field)
    }

    override fun deserialize(input: DataInput): Wide {
        val nulls = RecordFormat.readNullBitmap(input, 9)
        val values = Array(9) { if (nulls[it]) null else RecordFormat.readBoolean(input) }
        return Wide(
            i = values[0], h = values[1], g = values[2], f = values[3], e = values[4],
            d = values[5], c = values[6], b = values[7], a = values[8],
        )
    }

    override fun copy(value: Wide): Wide = value
}

/** One field of every built-in kind. */
data class Scalars(
    val z: Boolean,
    val b: Byte,
    val s: Short,
    val i: Int,
    val l: Long,
    val f: Float,
    val d: Double,
    val c: Char,
    val t: String,
)

/** Persisted order: `z`, `b`, `s`, `i`, `l`, `f`, `d`, `c`, `t`, the constructor's. */
object ScalarsCodec : GeneratedCodec<Scalars> {
    override fun serialize(value: Scalars, output: DataOutput) {
        RecordFormat.writeBoolean(output, value.z)
        RecordFormat.writeByte(output, value.b)
        RecordFormat.writeShort(output, value.s)
        RecordFormat.writeInt(output, value.i)
        RecordFormat.writeLong(output, value.l)
        RecordFormat.writeFloat(output, value.f)
        RecordFormat.writeDouble(output, value.d)
        RecordFormat.writeChar(output, value.c)
        RecordFormat.writeString(output, value.t)
    }

    override fun deserialize(input: DataInput): Scalars {
        val z = RecordFormat.readBoolean(input)
        val b = RecordFormat.readByte(input)
        val s = RecordFormat.readShort(input)
        val i = RecordFormat.readInt(input)
        val l = RecordFormat.readLong(input)
        val f = RecordFormat.readFloat(input)
        val d = RecordFormat.readDouble(input)
        val c = RecordFormat.readChar(input)
        val t = RecordFormat.readString(input)
        return Scalars(z = z, b = b, s = s, i = i, l = l, f = f, d = d, c = c, t = t)
    }

    override fun copy(value: Scalars): Scalars = value
}

/** A top-level nullable type, such as `typeInfo<User?>()`: one marker byte, then the non-null type's bytes. */
class NullableCodec<T : Any>(private val inner: GeneratedCodec<T>) : GeneratedCodec<T?> {
    override fun serialize(value: T?, output: DataOutput) {
        RecordFormat.writeNullMarker(output, value == null)
        if (value != null) inner.serialize(value, output)
    }

    override fun deserialize(input: DataInput): T? = if (RecordFormat.readNullMarker(input)) inner.deserialize(input) else null

    override fun copy(value: T?): T? = value?.let(inner::copy)
}

/**
 * A generic record, whose bytes these are if #22 supports generic classes. `previous` is declared `T?`, so it has a
 * bitmap bit whatever `T` is, and `value` is declared `T`, so it has none. Both are written by the argument's codec,
 * which for a nullable argument writes a null marker.
 */
data class Box<T>(val value: T, val previous: T?)

/**
 * Persisted order: `value`, `previous`. Generated once, and given the argument's codec, as #22 would compose it.
 *
 * A present `previous` has a clear bitmap bit, and a nullable argument's codec then writes `01` before it. Reading it,
 * the argument's codec returns `null` only for a `00` marker: a second encoding of null, which no writer produces,
 * so the read fails.
 */
class BoxCodec<T>(private val argument: GeneratedCodec<T>) : GeneratedCodec<Box<T>> {
    override fun serialize(value: Box<T>, output: DataOutput) {
        val previous = value.previous
        output.writeByte(if (previous == null) 0x01 else 0)
        argument.serialize(value.value, output)
        if (previous != null) argument.serialize(previous, output)
    }

    override fun deserialize(input: DataInput): Box<T> {
        val nulls = input.readUnsignedByte()
        if (nulls and 0xfe != 0) throw MalformedDataException("Box's null bitmap has 1 bit, not ${"%02x".format(nulls)}")
        val value = argument.deserialize(input)
        val previous = if (nulls and 0x01 != 0) null else argument.deserialize(input) ?: throw RecordFormat.nullInPresentField("Box.previous")
        return Box(value = value, previous = previous)
    }

    override fun copy(value: Box<T>): Box<T> = value
}

/** A value whose bytes have been written. */
fun <T> GeneratedCodec<T>.write(value: T): Written = written { serialize(value, it) }
