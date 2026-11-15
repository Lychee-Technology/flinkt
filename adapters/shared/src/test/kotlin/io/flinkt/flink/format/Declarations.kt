package io.flinkt.flink.format

import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import java.io.File

/**
 * Versions of one job's declarations, each compiled into a class loader of its own, as two releases of a job are:
 * the old one writes state, and the current one restores it with only its own classes. Each version has KSP-style
 * codecs written by hand, a `GeneratedTypeModule` registered in `META-INF/services`, and a `Samples` class that
 * builds its values.
 */
object Declarations {
    /** Compiles [source] with [compiler], against the module's API and this prototype, and loads it on its own. */
    fun load(source: String, compiler: KotlinCompiler = KotlinCompiler.BUILD): ClassLoader {
        val prototype = File(RecordFormat::class.java.protectionDomain.codeSource.location.toURI())
        val result = KotlinCompilation(compiler, classpath = KotlinCompilation().classpath + prototype)
            .compile(HEADER + source.trimIndent())
            .assertSuccess()
        File(result.classesDir, "META-INF/services/io.flinkt.registry.GeneratedTypeModule")
            .apply { parentFile.mkdirs() }
            .writeText("com.example.Types\n")
        return result.classLoader()
    }

    /** The values the version's `Samples` class builds. */
    fun samples(loader: ClassLoader): List<Any> {
        @Suppress("UNCHECKED_CAST")
        val samples = loader.loadClass("com.example.Samples").getDeclaredConstructor().newInstance() as () -> List<Any>
        return samples()
    }

    private val HEADER = """
        package com.example

        import io.flinkt.FlinkType
        import io.flinkt.codec.CodecFactory
        import io.flinkt.codec.GeneratedCodec
        import io.flinkt.codec.codec
        import io.flinkt.flink.format.MigrationConstructor
        import io.flinkt.flink.format.PersistedFieldOrder
        import io.flinkt.flink.format.RecordFormat
        import io.flinkt.registry.GeneratedTypeModule
        import io.flinkt.registry.MutableTypeRegistry
        import io.flinkt.schema.FieldSchema
        import io.flinkt.schema.RecordSchema
        import io.flinkt.schema.SchemaType
        import java.io.DataInput
        import java.io.DataOutput

        fun type(name: String, nullable: Boolean = false) = SchemaType.ClassType(name, isNullable = nullable)

    """.trimIndent() + "\n"

    /** `User(id: Long, name: String)`, the current declaration of every restore. */
    val USER = """
        @FlinkType
        data class User(val id: Long, val name: String)

        object UserCodec : GeneratedCodec<User>, MigrationConstructor<User> {
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

            override val parameterNames = listOf("id", "name")

            override fun construct(arguments: Array<Any?>): User = User(arguments[0] as Long, arguments[1] as String)
        }

        class Types : GeneratedTypeModule {
            override fun register(registry: MutableTypeRegistry) {
                registry.register(
                    User::class,
                    RecordSchema("com.example.User", listOf(FieldSchema("id", type("kotlin.Long")), FieldSchema("name", type("kotlin.String")))),
                    CodecFactory { UserCodec },
                )
            }
        }

        class Samples : () -> List<Any> {
            override fun invoke(): List<Any> = listOf(User(1, "Ada"), User(2, "Grace"))
        }
    """

    /**
     * `User(id: Long)`. Its second sample's bytes start with `04 41 64 61`, which the current codec would read as the
     * first sample's name.
     */
    val USER_ID_ONLY = """
        @FlinkType
        data class User(val id: Long)

        object UserCodec : GeneratedCodec<User> {
            override fun serialize(value: User, output: DataOutput) = RecordFormat.writeLong(output, value.id)

            override fun deserialize(input: DataInput): User = User(id = RecordFormat.readLong(input))

            override fun copy(value: User): User = value
        }

        class Types : GeneratedTypeModule {
            override fun register(registry: MutableTypeRegistry) {
                registry.register(User::class, RecordSchema("com.example.User", listOf(FieldSchema("id", type("kotlin.Long")))), CodecFactory { UserCodec })
            }
        }

        class Samples : () -> List<Any> {
            override fun invoke(): List<Any> = listOf(User(1), User(0x0441646100000000L))
        }
    """

    /** `User(id: Long, name: String, profile: OldProfile)`, where `OldProfile` has a nullable nested `Address`. */
    val USER_WITH_PROFILE = """
        @FlinkType
        data class Address(val city: String, val zip: Int)

        @FlinkType
        data class OldProfile(val bio: String?, val score: Double, val home: Address?)

        @FlinkType
        data class User(val id: Long, val name: String, val profile: OldProfile)

        object AddressCodec : GeneratedCodec<Address> {
            override fun serialize(value: Address, output: DataOutput) {
                RecordFormat.writeString(output, value.city)
                RecordFormat.writeInt(output, value.zip)
            }

            override fun deserialize(input: DataInput): Address {
                val city = RecordFormat.readString(input)
                val zip = RecordFormat.readInt(input)
                return Address(city = city, zip = zip)
            }

            override fun copy(value: Address): Address = value
        }

        // Persisted order: bio, score, home, the constructor's. The bitmap: bio is bit 0, home bit 1.
        class OldProfileCodec(private val address: GeneratedCodec<Address>) : GeneratedCodec<OldProfile> {
            override fun serialize(value: OldProfile, output: DataOutput) {
                val bio = value.bio
                val home = value.home
                output.writeByte((if (bio == null) 1 else 0) or (if (home == null) 2 else 0))
                if (bio != null) RecordFormat.writeString(output, bio)
                RecordFormat.writeDouble(output, value.score)
                if (home != null) address.serialize(home, output)
            }

            override fun deserialize(input: DataInput): OldProfile {
                val nulls = input.readUnsignedByte()
                val bio = if (nulls and 1 != 0) null else RecordFormat.readString(input)
                val score = RecordFormat.readDouble(input)
                val home = if (nulls and 2 != 0) null else address.deserialize(input)
                return OldProfile(bio = bio, score = score, home = home)
            }

            override fun copy(value: OldProfile): OldProfile = value
        }

        class UserCodec(private val profile: GeneratedCodec<OldProfile>) : GeneratedCodec<User> {
            override fun serialize(value: User, output: DataOutput) {
                RecordFormat.writeLong(output, value.id)
                RecordFormat.writeString(output, value.name)
                profile.serialize(value.profile, output)
            }

            override fun deserialize(input: DataInput): User {
                val id = RecordFormat.readLong(input)
                val name = RecordFormat.readString(input)
                val profile = profile.deserialize(input)
                return User(id = id, name = name, profile = profile)
            }

            override fun copy(value: User): User = value
        }

        class Types : GeneratedTypeModule {
            override fun register(registry: MutableTypeRegistry) {
                registry.register(
                    Address::class,
                    RecordSchema("com.example.Address", listOf(FieldSchema("city", type("kotlin.String")), FieldSchema("zip", type("kotlin.Int")))),
                    CodecFactory { AddressCodec },
                )
                registry.register(
                    OldProfile::class,
                    RecordSchema(
                        "com.example.OldProfile",
                        listOf(
                            FieldSchema("bio", type("kotlin.String", nullable = true)),
                            FieldSchema("score", type("kotlin.Double")),
                            FieldSchema("home", type("com.example.Address", nullable = true)),
                        ),
                    ),
                    CodecFactory { context -> OldProfileCodec(context.codec<Address>()) },
                )
                registry.register(
                    User::class,
                    RecordSchema(
                        "com.example.User",
                        listOf(
                            FieldSchema("id", type("kotlin.Long")),
                            FieldSchema("name", type("kotlin.String")),
                            FieldSchema("profile", type("com.example.OldProfile")),
                        ),
                    ),
                    CodecFactory { context -> UserCodec(context.codec<OldProfile>()) },
                )
            }
        }

        class Samples : () -> List<Any> {
            override fun invoke(): List<Any> = listOf(
                User(1, "Ada", OldProfile("pioneer \uD800", 9.5, Address("London", 1))),
                User(2, "Grace", OldProfile(null, Double.NaN, null)),
                User(3, "Barbara", OldProfile("", -0.0, Address("Zürich", 8000))),
            )
        }
    """

    /** `User(id: Long, name: String, tags: Map<String, Long>)`. */
    val USER_WITH_TAGS = """
        @FlinkType
        data class User(val id: Long, val name: String, val tags: Map<String, Long>)

        object UserCodec : GeneratedCodec<User> {
            override fun serialize(value: User, output: DataOutput) {
                RecordFormat.writeLong(output, value.id)
                RecordFormat.writeString(output, value.name)
                RecordFormat.writeMap(output, value.tags, { out, k -> RecordFormat.writeString(out, k) }, { out, v -> RecordFormat.writeLong(out, v) })
            }

            override fun deserialize(input: DataInput): User {
                val id = RecordFormat.readLong(input)
                val name = RecordFormat.readString(input)
                val tags = RecordFormat.readMap(input, { RecordFormat.readString(it) }, { RecordFormat.readLong(it) })
                return User(id = id, name = name, tags = tags)
            }

            override fun copy(value: User): User = value
        }

        class Types : GeneratedTypeModule {
            override fun register(registry: MutableTypeRegistry) {
                val tags = SchemaType.ClassType(
                    "kotlin.collections.Map",
                    listOf(io.flinkt.schema.SchemaTypeArgument.invariant(type("kotlin.String")), io.flinkt.schema.SchemaTypeArgument.invariant(type("kotlin.Long"))),
                )
                registry.register(
                    User::class,
                    RecordSchema(
                        "com.example.User",
                        listOf(FieldSchema("id", type("kotlin.Long")), FieldSchema("name", type("kotlin.String")), FieldSchema("tags", tags)),
                    ),
                    CodecFactory { UserCodec },
                )
            }
        }

        class Samples : () -> List<Any> {
            override fun invoke(): List<Any> = listOf(User(1, "Ada", mapOf("a" to 1L, "b" to 2L)), User(2, "Grace", emptyMap()))
        }
    """

    /** `Key(a: Int, b: Int)`, with an `init` block that checks both arguments. Persisted order: the constructor's. */
    val KEY_AB = key(listOf("a", "b"))

    /** The same key with its constructor parameters reordered, `Key(b: Int, a: Int)`, which reorders its layout too. */
    val KEY_BA = key(listOf("b", "a"))

    /** `Key(a: Int, b: Int)` with `@PersistedFieldOrder` keys 10 and 20: persisted order `a`, `b`, as [KEY_AB]'s. */
    val KEY_AB_PINNED = key(listOf("a", "b"), mapOf("a" to 10, "b" to 20))

    /** `Key(b: Int, a: Int)`, its keys renumbered to 7 and 3: persisted order `a`, `b` still, a constructor reorder only. */
    val KEY_BA_PINNED = key(listOf("b", "a"), mapOf("b" to 7, "a" to 3))

    /**
     * `Holder(keys: Map<Key, Boolean>)`, with [KEY_AB_PINNED]'s key, and a sample of 1,025 keys `Key(-31 * i, i)`: their
     * hash codes, `31 * a + b`, are `-960 * i`, all different. A constructor reorder makes them `31 * b + a`, all 0.
     */
    val HOLDER_AB_PINNED = key(listOf("a", "b"), mapOf("a" to 10, "b" to 20), holder = true)

    /** [HOLDER_AB_PINNED] with the key's constructor reordered, as [KEY_BA_PINNED] is: its keys' hash codes change. */
    val HOLDER_BA_PINNED = key(listOf("b", "a"), mapOf("b" to 7, "a" to 3), holder = true)

    /** [HOLDER_AB_PINNED] with its keys renumbered to 1 and 1,000, which changes no order: an identical schema. */
    val HOLDER_AB_RENUMBERED = key(listOf("a", "b"), mapOf("a" to 1, "b" to 1_000), holder = true)

    /**
     * `Key`, with the constructor parameters [constructor], and `@PersistedFieldOrder` keys [pins] if any. Its codec
     * writes the persisted order, which is the constructor's without pins, reads each field into a local in that
     * order, and then calls the constructor once, by name. With [holder], the module also has `Holder`, whose map's
     * keys its codec counts by hash code, and its sample is a `Holder`.
     */
    private fun key(constructor: List<String>, pins: Map<String, Int>? = null, holder: Boolean = false): String {
        val persisted = pins?.entries?.sortedBy { it.value }?.map { it.key } ?: constructor
        val parameters = constructor.joinToString { name -> (pins?.get(name)?.let { "@property:PersistedFieldOrder(order = $it) " } ?: "") + "val $name: Int" }
        val schema = "RecordSchema(\"com.example.Key\", listOf(${constructor.joinToString { "FieldSchema(\"$it\", type(\"kotlin.Int\"))" }}), " +
            "persistedOrder = listOf(${persisted.joinToString { "\"$it\"" }}))"
        val holderDeclarations = if (!holder) "" else """
            @FlinkType
            data class Holder(val keys: Map<Key, Boolean>)

            class HolderCodec(private val key: GeneratedCodec<Key>) : GeneratedCodec<Holder> {
                override fun serialize(value: Holder, output: DataOutput) =
                    RecordFormat.writeMap(output, value.keys, { out, k -> key.serialize(k, out) }, RecordFormat::writeBoolean) { true }

                override fun deserialize(input: DataInput): Holder =
                    Holder(keys = RecordFormat.readMap(input, { key.deserialize(it) }, RecordFormat::readBoolean) { true })

                override fun copy(value: Holder): Holder = value
            }
        """
        val holderRegistration = if (!holder) "" else """
                    val keys = SchemaType.ClassType(
                        "kotlin.collections.Map",
                        listOf(io.flinkt.schema.SchemaTypeArgument.invariant(type("com.example.Key")), io.flinkt.schema.SchemaTypeArgument.invariant(type("kotlin.Boolean"))),
                    )
                    registry.register(Holder::class, RecordSchema("com.example.Holder", listOf(FieldSchema("keys", keys))), CodecFactory { context -> HolderCodec(context.codec<Key>()) })
        """
        val sample = if (holder) "Holder((0..1024).associate { Key(a = -31 * it, b = it) to true })" else "Key(a = 1, b = 2)"
        return """
            @FlinkType
            data class Key($parameters) {
                init {
                    require(a <= b) { "a must not exceed b" }
                }
            }

            // Persisted order: ${persisted.joinToString()}. Each field is read into a local, then the constructor is
            // called once.
            object KeyCodec : GeneratedCodec<Key>, MigrationConstructor<Key> {
                override fun serialize(value: Key, output: DataOutput) {
                    ${persisted.joinToString("\n                    ") { "RecordFormat.writeInt(output, value.$it)" }}
                }

                override fun deserialize(input: DataInput): Key {
                    ${persisted.joinToString("\n                    ") { "val $it = RecordFormat.readInt(input)" }}
                    return Key(${constructor.joinToString { "$it = $it" }})
                }

                override fun copy(value: Key): Key = value

                override val parameterNames = listOf(${constructor.joinToString { "\"$it\"" }})

                override fun construct(arguments: Array<Any?>): Key = Key(arguments[0] as Int, arguments[1] as Int)
            }
            $holderDeclarations
            class Types : GeneratedTypeModule {
                override fun register(registry: MutableTypeRegistry) {
                    registry.register(Key::class, $schema, CodecFactory { KeyCodec })
                    $holderRegistration
                }
            }

            class Samples : () -> List<Any> {
                override fun invoke(): List<Any> = listOf($sample)
            }
        """
    }
}
