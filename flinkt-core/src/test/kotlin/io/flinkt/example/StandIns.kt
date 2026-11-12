package io.flinkt.example

import io.flinkt.FlinkType
import io.flinkt.codec.CodecFactory
import io.flinkt.codec.GeneratedCodec
import io.flinkt.codec.codec
import io.flinkt.registry.GeneratedTypeModule
import io.flinkt.registry.MutableTypeRegistry
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
import java.io.DataInput
import java.io.DataOutput
import java.util.concurrent.atomic.AtomicInteger

// Hand-written stand-ins for what flinkt-ksp is to generate. Their bytes are a test's choice, not a format: the
// record layout and the scalar encodings are #5's.

@FlinkType
data class User(val id: Long, val name: String)

@FlinkType
data class Order(val id: Long, val user: User)

@FlinkType
data class Envelope<T>(val timestamp: Long, val payload: T)

/** A record whose field type this release can't resolve. */
@FlinkType
data class Contact(val user: User?)

/** A record that contains itself, so its codec would need itself. */
@FlinkType
class Loop(val next: Loop)

@FlinkType
@JvmInline
value class UserId(val value: Long)

@FlinkType
enum class Color { RED, GREEN }

@FlinkType
sealed interface Event

@FlinkType
data class Created(val user: User) : Event

@FlinkType
data object Shutdown : Event

/** Annotated, but no module registers it, as when flinkt-ksp didn't run. */
@FlinkType
data class Unregistered(val id: Long)

/** Not annotated. */
data class Plain(val id: Long)

class Outer {
    @FlinkType
    data class Inner(val id: Long)
}

object UserCodec : GeneratedCodec<User> {
    override fun serialize(value: User, output: DataOutput) {
        output.writeLong(value.id)
        output.writeUTF(value.name)
    }

    override fun deserialize(input: DataInput): User = User(input.readLong(), input.readUTF())

    override fun copy(value: User): User = value
}

/** Counts how often the codec of [User] is created, to show that a record's codec creates its nested codecs once. */
object UserCodecFactory : CodecFactory<User> {
    val created = AtomicInteger()

    override fun create(context: io.flinkt.codec.CodecContext): GeneratedCodec<User> {
        created.incrementAndGet()
        return UserCodec
    }
}

/** The codec of [Order], which takes the codec of its `user` field when it's created. */
class OrderCodec(private val user: GeneratedCodec<User>) : GeneratedCodec<Order> {
    override fun serialize(value: Order, output: DataOutput) {
        output.writeLong(value.id)
        user.serialize(value.user, output)
    }

    override fun deserialize(input: DataInput): Order = Order(input.readLong(), user.deserialize(input))

    override fun copy(value: Order): Order = Order(value.id, user.copy(value.user))
}

object IntCodec : GeneratedCodec<Int> {
    override fun serialize(value: Int, output: DataOutput): Unit = output.writeInt(value)

    override fun deserialize(input: DataInput): Int = input.readInt()

    override fun copy(value: Int): Int = value
}

object StringCodec : GeneratedCodec<String> {
    override fun serialize(value: String, output: DataOutput): Unit = output.writeUTF(value)

    override fun deserialize(input: DataInput): String = input.readUTF()

    override fun copy(value: String): String = value
}

private val LONG = SchemaType.ClassType("kotlin.Long")
private val STRING = SchemaType.ClassType("kotlin.String")
private val USER = SchemaType.ClassType(User::class.java.name)

val userSchema = RecordSchema(User::class.java.name, listOf(FieldSchema("id", LONG), FieldSchema("name", STRING)))

/** The stand-in for a module that flinkt-ksp generates for one compilation. */
class StandInTypes : GeneratedTypeModule {
    override fun register(registry: MutableTypeRegistry) {
        registry.register(User::class, userSchema, UserCodecFactory)
        registry.register(
            Order::class,
            RecordSchema(Order::class.java.name, listOf(FieldSchema("id", LONG), FieldSchema("user", USER))),
        ) { context -> OrderCodec(context.codec()) }
        registry.register(
            Envelope::class,
            RecordSchema(
                Envelope::class.java.name,
                listOf(FieldSchema("timestamp", LONG), FieldSchema("payload", SchemaType.TypeParameter("T"))),
                typeParameters = listOf("T"),
            ),
        ) { error("generic classes aren't resolved yet") }
        registry.register(
            Contact::class,
            RecordSchema(Contact::class.java.name, listOf(FieldSchema("user", SchemaType.ClassType(User::class.java.name, isNullable = true)))),
        ) { context ->
            context.codec<User?>()
            error("unreachable: User? isn't resolved yet")
        }
        registry.register(
            Loop::class,
            RecordSchema(Loop::class.java.name, listOf(FieldSchema("next", SchemaType.ClassType(Loop::class.java.name)))),
        ) { context ->
            context.codec<Loop>()
            error("unreachable: Loop contains itself")
        }
        registry.register(
            UserId::class,
            ValueClassSchema(UserId::class.java.name, FieldSchema("value", LONG)),
        ) { error("value classes aren't resolved yet") }
        registry.register(
            Color::class,
            EnumSchema(Color::class.java.name, listOf(EnumConstantSchema("RED", 1), EnumConstantSchema("GREEN", 2))),
        ) { error("enums aren't resolved yet") }
        registry.register(
            Event::class,
            SealedSchema(
                Event::class.java.name,
                listOf(
                    SealedSubtypeSchema(1, SchemaType.ClassType(Created::class.java.name)),
                    SealedSubtypeSchema(2, SchemaType.ClassType(Shutdown::class.java.name)),
                ),
            ),
        ) { error("sealed types aren't resolved yet") }
        registry.register(
            Created::class,
            RecordSchema(Created::class.java.name, listOf(FieldSchema("user", USER))),
        ) { context -> error("not needed: ${context.type}") }
        registry.register(Shutdown::class, ObjectSchema(Shutdown::class.java.name)) { error("objects aren't resolved yet") }
        registry.register(
            Outer.Inner::class,
            RecordSchema(Outer.Inner::class.java.name, listOf(FieldSchema("id", LONG))),
        ) { error("not needed") }
    }
}

/** Stand-in codecs for two built-in types. Flinkt's core registers none, because their bytes are #5's. */
class StandInBuiltIns : GeneratedTypeModule {
    override fun register(registry: MutableTypeRegistry) {
        registry.registerBuiltIn(Int::class, CodecFactory { IntCodec })
        registry.registerBuiltIn(String::class, CodecFactory { StringCodec })
    }
}

/** A schema with a `List<User?>` field, to show that a schema keeps every argument and nullability flag. */
val listOfNullableUsers = SchemaType.ClassType(
    "kotlin.collections.List",
    listOf(SchemaTypeArgument.invariant(SchemaType.ClassType(User::class.java.name, isNullable = true))),
)
