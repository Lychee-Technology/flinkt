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

        // Canonical order: bio, home, score. The bitmap: bio is bit 0, home bit 1.
        class OldProfileCodec(private val address: GeneratedCodec<Address>) : GeneratedCodec<OldProfile> {
            override fun serialize(value: OldProfile, output: DataOutput) {
                val bio = value.bio
                val home = value.home
                output.writeByte((if (bio == null) 1 else 0) or (if (home == null) 2 else 0))
                if (bio != null) RecordFormat.writeString(output, bio)
                if (home != null) address.serialize(home, output)
                RecordFormat.writeDouble(output, value.score)
            }

            override fun deserialize(input: DataInput): OldProfile {
                val nulls = input.readUnsignedByte()
                val bio = if (nulls and 1 != 0) null else RecordFormat.readString(input)
                val home = if (nulls and 2 != 0) null else address.deserialize(input)
                val score = RecordFormat.readDouble(input)
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

    /** `Key(a: Int, b: Int)`, with an `init` block that checks both arguments. */
    val KEY_AB = key("val a: Int, val b: Int", "Key(a = a, b = b)", "Key(1, 2)", "arguments[0] as Int, arguments[1] as Int")

    /** The same key with its constructor parameters reordered: `Key(b: Int, a: Int)`. */
    val KEY_BA = key("val b: Int, val a: Int", "Key(b = b, a = a)", "Key(2, 1)", "arguments[0] as Int, arguments[1] as Int")

    private fun key(parameters: String, construct: String, sample: String, arguments: String): String {
        val fields = parameters.split(", ").map { it.removePrefix("val ").substringBefore(':') }
        return """
            @FlinkType
            data class Key($parameters) {
                init {
                    require(a <= b) { "a must not exceed b" }
                }
            }

            // Canonical order: a, b, whatever the constructor order. Each is read into a local, then the constructor
            // is called once.
            object KeyCodec : GeneratedCodec<Key>, MigrationConstructor<Key> {
                override fun serialize(value: Key, output: DataOutput) {
                    RecordFormat.writeInt(output, value.a)
                    RecordFormat.writeInt(output, value.b)
                }

                override fun deserialize(input: DataInput): Key {
                    val a = RecordFormat.readInt(input)
                    val b = RecordFormat.readInt(input)
                    return $construct
                }

                override fun copy(value: Key): Key = value

                override val parameterNames = listOf(${fields.joinToString { "\"$it\"" }})

                override fun construct(arguments: Array<Any?>): Key = Key($arguments)
            }

            class Types : GeneratedTypeModule {
                override fun register(registry: MutableTypeRegistry) {
                    registry.register(
                        Key::class,
                        RecordSchema("com.example.Key", listOf(${fields.joinToString { "FieldSchema(\"$it\", type(\"kotlin.Int\"))" }})),
                        CodecFactory { KeyCodec },
                    )
                }
            }

            class Samples : () -> List<Any> {
                override fun invoke(): List<Any> = listOf($sample)
            }
        """
    }
}
