package io.flinkt.flink.format

import io.flinkt.codec.GeneratedCodec
import java.io.DataInput
import java.io.DataOutput

/*
 * The decision's byte-level examples (docs/persisted-format.md, "Examples"), with codecs written by hand the way KSP
 * will generate them (#11): fields in canonical order, ascending String.compareTo of the property names, each read
 * into a local, then one call of the primary constructor in declaration order. #12 and #15 turn the examples into
 * golden fixtures.
 */

data class User(val id: Long, val name: String)

/** Canonical order: `id`, `name`. No nullable field, so no bitmap. */
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

/** The nullable variant. Its constructor order isn't its canonical order. */
data class Account(
    val id: Long,
    val name: String?,
    val email: String?,
    val balance: Double?,
    val rating: Float,
)

/**
 * Canonical order: `balance`, `email`, `id`, `name`, `rating`. The nullable fields in that order are `balance`,
 * `email` and `name`: bits 0, 1 and 2 of a one-byte bitmap, which comes first.
 */
object AccountCodec : GeneratedCodec<Account> {
    override fun serialize(value: Account, output: DataOutput) {
        val balance = value.balance
        val email = value.email
        val name = value.name
        var nulls = 0
        if (balance == null) nulls = nulls or 0x01
        if (email == null) nulls = nulls or 0x02
        if (name == null) nulls = nulls or 0x04
        output.writeByte(nulls)
        if (balance != null) RecordFormat.writeDouble(output, balance)
        if (email != null) RecordFormat.writeString(output, email)
        RecordFormat.writeLong(output, value.id)
        if (name != null) RecordFormat.writeString(output, name)
        RecordFormat.writeFloat(output, value.rating)
    }

    override fun deserialize(input: DataInput): Account {
        val nulls = input.readUnsignedByte()
        if (nulls and 0xf8 != 0) throw MalformedDataException("Account's null bitmap has 3 bits, not ${"%02x".format(nulls)}")
        val balance = if (nulls and 0x01 != 0) null else RecordFormat.readDouble(input)
        val email = if (nulls and 0x02 != 0) null else RecordFormat.readString(input)
        val id = RecordFormat.readLong(input)
        val name = if (nulls and 0x04 != 0) null else RecordFormat.readString(input)
        val rating = RecordFormat.readFloat(input)
        return Account(id = id, name = name, email = email, balance = balance, rating = rating)
    }

    override fun copy(value: Account): Account = value
}

/** A record with nested records, one of them nullable. */
data class Session(val user: User, val note: String?, val previous: User?)

/** Canonical order: `note`, `previous`, `user`. Nested records are written inline, with no length or header. */
object SessionCodec : GeneratedCodec<Session> {
    override fun serialize(value: Session, output: DataOutput) {
        val note = value.note
        val previous = value.previous
        var nulls = 0
        if (note == null) nulls = nulls or 0x01
        if (previous == null) nulls = nulls or 0x02
        output.writeByte(nulls)
        if (note != null) RecordFormat.writeString(output, note)
        if (previous != null) UserCodec.serialize(previous, output)
        UserCodec.serialize(value.user, output)
    }

    override fun deserialize(input: DataInput): Session {
        val nulls = input.readUnsignedByte()
        if (nulls and 0xfc != 0) throw MalformedDataException("Session's null bitmap has 2 bits, not ${"%02x".format(nulls)}")
        val note = if (nulls and 0x01 != 0) null else RecordFormat.readString(input)
        val previous = if (nulls and 0x02 != 0) null else UserCodec.deserialize(input)
        val user = UserCodec.deserialize(input)
        return Session(user = user, note = note, previous = previous)
    }

    override fun copy(value: Session): Session = value
}

/** Nine nullable fields, declared out of canonical order: a two-byte bitmap. */
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

/** Canonical order `a` to `i`: bits 0 to 7 of the first bitmap byte, then bit 0 of the second. */
object WideCodec : GeneratedCodec<Wide> {
    private fun Wide.fields(): List<Boolean?> = listOf(a, b, c, d, e, f, g, h, i)

    override fun serialize(value: Wide, output: DataOutput) {
        val fields = value.fields()
        RecordFormat.writeNullBitmap(output, BooleanArray(fields.size) { fields[it] == null })
        for (field in fields) if (field != null) RecordFormat.writeBoolean(output, field)
    }

    override fun deserialize(input: DataInput): Wide {
        val nulls = RecordFormat.readNullBitmap(input, 9)
        val values = Array(9) { if (nulls[it]) null else RecordFormat.readBoolean(input) }
        return Wide(
            a = values[0], b = values[1], c = values[2], d = values[3], e = values[4],
            f = values[5], g = values[6], h = values[7], i = values[8],
        )
    }

    override fun copy(value: Wide): Wide = value
}

/** One field of every built-in kind, declared out of canonical order. */
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

/** Canonical order: `b`, `c`, `d`, `f`, `i`, `l`, `s`, `t`, `z`. */
object ScalarsCodec : GeneratedCodec<Scalars> {
    override fun serialize(value: Scalars, output: DataOutput) {
        RecordFormat.writeByte(output, value.b)
        RecordFormat.writeChar(output, value.c)
        RecordFormat.writeDouble(output, value.d)
        RecordFormat.writeFloat(output, value.f)
        RecordFormat.writeInt(output, value.i)
        RecordFormat.writeLong(output, value.l)
        RecordFormat.writeShort(output, value.s)
        RecordFormat.writeString(output, value.t)
        RecordFormat.writeBoolean(output, value.z)
    }

    override fun deserialize(input: DataInput): Scalars {
        val b = RecordFormat.readByte(input)
        val c = RecordFormat.readChar(input)
        val d = RecordFormat.readDouble(input)
        val f = RecordFormat.readFloat(input)
        val i = RecordFormat.readInt(input)
        val l = RecordFormat.readLong(input)
        val s = RecordFormat.readShort(input)
        val t = RecordFormat.readString(input)
        val z = RecordFormat.readBoolean(input)
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
 * Canonical order: `previous`, `value`. Generated once, and given the argument's codec, as #22 would compose it.
 *
 * A present `previous` has a clear bitmap bit, and a nullable argument's codec then writes `01` before it. Reading it,
 * the argument's codec returns `null` only for a `00` marker: a second encoding of null, which no writer produces,
 * so the read fails.
 */
class BoxCodec<T>(private val argument: GeneratedCodec<T>) : GeneratedCodec<Box<T>> {
    override fun serialize(value: Box<T>, output: DataOutput) {
        val previous = value.previous
        output.writeByte(if (previous == null) 0x01 else 0)
        if (previous != null) argument.serialize(previous, output)
        argument.serialize(value.value, output)
    }

    override fun deserialize(input: DataInput): Box<T> {
        val nulls = input.readUnsignedByte()
        if (nulls and 0xfe != 0) throw MalformedDataException("Box's null bitmap has 1 bit, not ${"%02x".format(nulls)}")
        val previous = if (nulls and 0x01 != 0) null else argument.deserialize(input) ?: throw RecordFormat.nullInPresentField("Box.previous")
        val value = argument.deserialize(input)
        return Box(value = value, previous = previous)
    }

    override fun copy(value: Box<T>): Box<T> = value
}

/** A value whose bytes have been written. */
fun <T> GeneratedCodec<T>.write(value: T): Written = written { serialize(value, it) }
