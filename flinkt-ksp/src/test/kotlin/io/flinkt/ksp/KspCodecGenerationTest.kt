package io.flinkt.ksp

import io.flinkt.ksp.KspTestSupport.codecFor
import io.flinkt.ksp.KspTestSupport.compile
import io.flinkt.ksp.KspTestSupport.compileSingle
import io.flinkt.ksp.KspTestSupport.deserialize
import io.flinkt.ksp.KspTestSupport.hex
import io.flinkt.ksp.KspTestSupport.roundTrip
import io.flinkt.ksp.KspTestSupport.serialize
import io.flinkt.registry.TypeRegistry
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.testing.ClassFiles
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.Origin
import io.flinkt.testing.compile.SourceFile
import java.io.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * End-to-end KSP code generation, golden bytes, schema metadata, positive rule-table forms, runtime without
 * `kotlin-reflect`, and minimum Kotlin language version warnings.
 */
class KspCodecGenerationTest {
    @Test
    fun `published flinkt-ksp classes target Java 8 and carry Kotlin metadata 2_4_0`() {
        assertEquals(52, ClassFiles.majorVersion(FlinktSymbolProcessorProvider::class.java))
        assertEquals("2.4.0", ClassFiles.kotlinMetadataVersion(FlinktSymbolProcessorProvider::class.java))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `User(id, name) compiles, matches #5 golden bytes, registers schema, and runs without kotlin-reflect`(
        compiler: KotlinCompiler,
    ) {
        val result = compileSingle(
            """
            package com.example

            import io.flinkt.FlinkType
            import io.flinkt.codec.GeneratedCodec
            import io.flinkt.registry.Resolution
            import io.flinkt.registry.TypeRegistry
            import io.flinkt.registry.resolve
            import java.io.ByteArrayInputStream
            import java.io.ByteArrayOutputStream
            import java.io.DataInputStream
            import java.io.DataOutputStream

            @FlinkType
            data class User(val id: Long, val name: String)

            @FlinkType
            data class Order(val id: Long, val user: User)

            object IsolatedRoundTrip {
                @JvmStatic
                fun run(): String {
                    val loader = User::class.java.classLoader
                    try {
                        Class.forName("kotlin.reflect.jvm.internal.KClassImpl", false, loader)
                        error("kotlin-reflect must not be on the isolated runtime classpath")
                    } catch (_: ClassNotFoundException) {
                        // expected
                    }
                    try {
                        Class.forName("kotlin.reflect.full.KClasses", false, loader)
                        error("kotlin-reflect must not be on the isolated runtime classpath")
                    } catch (_: ClassNotFoundException) {
                        // expected
                    }
                    val registry = TypeRegistry.load(loader)
                    @Suppress("UNCHECKED_CAST")
                    val orderCodec = (registry.resolve<Order>() as Resolution.Codec).codec as GeneratedCodec<Order>
                    val original = Order(7L, User(42L, "Ada"))
                    val bytes = ByteArrayOutputStream().also { orderCodec.serialize(original, DataOutputStream(it)) }.toByteArray()
                    val decoded = orderCodec.deserialize(DataInputStream(ByteArrayInputStream(bytes)))
                    check(decoded == original) { "decoded != original: ${'$'}decoded" }
                    check(orderCodec.copy(original) == original) { "copy != original" }
                    return bytes.joinToString(" ") { "%02x".format(it.toInt() and 0xff) }
                }
            }
            """,
            compiler = compiler,
        ).assertSuccess()

        // Run the full SPI resolution and round trip in a class loader with ONLY kotlin-stdlib, flinkt-core, and the
        // compiled classes (no kotlin-reflect, no Gradle TestKit, no KSP).
        val coreRuntime = System.getProperty("flinkt.test.coreRuntimeClasspath")
            .split(File.pathSeparatorChar)
            .map(::File)
        val isolatedUrls = (listOf(result.classesDir) + coreRuntime).map { it.toURI().toURL() }.toTypedArray()
        java.net.URLClassLoader(isolatedUrls, ClassLoader.getPlatformClassLoader()).use { isolated ->
            val runner = isolated.loadClass("com.example.IsolatedRoundTrip")
            val hexBytes = runner.getMethod("run").invoke(null) as String
            assertEquals("00 00 00 00 00 00 00 07 00 00 00 00 00 00 00 2a 04 41 64 61", hexBytes)
        }

        val loader = result.classLoader()
        val registry = TypeRegistry.load(loader)
        assertEquals(1, registry.modules.size)
        val userClass = loader.loadClass("com.example.User")
        val decl = registry.declaration(userClass)!!
        assertEquals(
            RecordSchema(
                typeName = "com.example.User",
                fields = listOf(
                    FieldSchema("id", SchemaType.ClassType("kotlin.Long")),
                    FieldSchema("name", SchemaType.ClassType("kotlin.String")),
                ),
            ),
            decl.schema,
        )

        val codec = codecFor(registry, loader, "com.example.User")
        val user = userClass.getConstructor(Long::class.javaPrimitiveType, String::class.java).newInstance(42L, "Ada")
        val bytes = serialize(codec, user)
        // Exact #5 golden bytes for User(42, "Ada")
        assertEquals("00 00 00 00 00 00 00 2a 04 41 64 61", hex(bytes))
        assertEquals(user, deserialize(codec, bytes))
        // Deeply immutable record (all val scalars) returns itself from copy().
        assertSame(user, codec.copy(user))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `every primitive type and String match #5 golden bytes at boundary values`(compiler: KotlinCompiler) {
        val result = compileSingle(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
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
            """,
            compiler = compiler,
        ).assertSuccess()

        val loader = result.classLoader()
        val registry = TypeRegistry.load(loader)
        val codec = codecFor(registry, loader, "com.example.Scalars")
        val ctor = loader.loadClass("com.example.Scalars").getConstructor(
            Boolean::class.javaPrimitiveType,
            Byte::class.javaPrimitiveType,
            Short::class.javaPrimitiveType,
            Int::class.javaPrimitiveType,
            Long::class.javaPrimitiveType,
            Float::class.javaPrimitiveType,
            Double::class.javaPrimitiveType,
            Char::class.javaPrimitiveType,
            String::class.java,
        )

        val sample1 = ctor.newInstance(
            true, (-1).toByte(), Short.MIN_VALUE, Int.MIN_VALUE, Long.MAX_VALUE,
            -0.0f, Double.MIN_VALUE, '\uD800', "\uDE00",
        )
        val bytes1 = serialize(codec, sample1)
        assertEquals(
            "01 ff 80 00 80 00 00 00 7f ff ff ff ff ff ff ff 80 00 00 00 00 00 00 00 00 00 00 01 d8 00 02 80 bc 03",
            hex(bytes1),
        )
        assertEquals(sample1, deserialize(codec, bytes1))

        val doubleNaN = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)
        val floatNaN = java.lang.Float.intBitsToFloat(0x7f800001)
        val sample2 = ctor.newInstance(
            false, Byte.MAX_VALUE, Short.MAX_VALUE, Int.MAX_VALUE, Long.MIN_VALUE,
            floatNaN, doubleNaN, Char.MAX_VALUE, "😀",
        )
        val bytes2 = serialize(codec, sample2)
        assertEquals(
            "00 7f 7f ff 7f ff ff ff 80 00 00 00 00 00 00 00 7f c0 00 00 7f f8 00 00 00 00 00 00 ff ff 03 bd b0 03 80 bc 03",
            hex(bytes2),
        )
        assertEquals(sample2, deserialize(codec, bytes2))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `nested generated types compose via CodecContext without copying layout, including nested classes and root package`(
        compiler: KotlinCompiler,
    ) {
        val result = compile(
            compiler,
            SourceFile(
                "com/example/Address.kt",
                """
                package com.example

                import io.flinkt.FlinkType

                @FlinkType
                data class Address(val city: String, var zip: Int)
                """.trimIndent() + "\n",
            ),
            SourceFile(
                "com/example/Order.kt",
                """
                package com.example

                import io.flinkt.FlinkType

                @FlinkType
                data class Order(val id: Long, val shipping: Address, val billing: Address)

                class OuterA {
                    @FlinkType
                    data class Inner(val x: Int)
                }

                class OuterB {
                    @FlinkType
                    data class Inner(val y: Long)
                }

                @FlinkType
                data class OuterA_1Inner(val z: String)
                """.trimIndent() + "\n",
            ),
            SourceFile(
                "RootRecord.kt",
                """
                import io.flinkt.FlinkType

                @FlinkType
                data class RootRecord(val id: Long, val order: com.example.Order)
                """.trimIndent() + "\n",
            ),
        ).assertSuccess()

        // Generated Order codec references Address by type and never copies Address's fields.
        val orderCodecSource = File(result.workDir, "ksp/kotlin/com/example/Order_FlinktCodec.kt").readText()
        assertFalse("city" in orderCodecSource, "Order codec must not inline Address's fields:\n$orderCodecSource")
        assertFalse("zip" in orderCodecSource, "Order codec must not inline Address's fields:\n$orderCodecSource")
        // Address is used twice in Order, but resolved only once in Factory.create.
        assertEquals(1, Regex("context\\._flinktCodec<com\\.example\\.Address>\\(\\)").findAll(orderCodecSource).count())

        val loader = result.classLoader()
        val registry = TypeRegistry.load(loader)

        val addressClass = loader.loadClass("com.example.Address")
        val orderClass = loader.loadClass("com.example.Order")
        val rootClass = loader.loadClass("RootRecord")

        // Verify schema uses JVM binary names (OuterA$Inner).
        assertEquals(
            "com.example.OuterA\$Inner",
            registry.declaration(loader.loadClass("com.example.OuterA\$Inner"))!!.schema.typeName,
        )
        assertEquals(
            "com.example.OuterB\$Inner",
            registry.declaration(loader.loadClass("com.example.OuterB\$Inner"))!!.schema.typeName,
        )
        assertEquals(
            "com.example.OuterA_1Inner",
            registry.declaration(loader.loadClass("com.example.OuterA_1Inner"))!!.schema.typeName,
        )

        val addr1 = addressClass.getConstructor(String::class.java, Int::class.javaPrimitiveType).newInstance("Berlin", 10115)
        val addr2 = addressClass.getConstructor(String::class.java, Int::class.javaPrimitiveType).newInstance("Munich", 80331)
        val order = orderClass.getConstructor(Long::class.javaPrimitiveType, addressClass, addressClass)
            .newInstance(99L, addr1, addr2)
        val root = rootClass.getConstructor(Long::class.javaPrimitiveType, orderClass).newInstance(7L, order)

        val orderCodec = codecFor(registry, loader, "com.example.Order")
        val rootCodec = codecFor(registry, loader, "RootRecord")
        assertEquals(order, roundTrip(orderCodec, order))
        assertEquals(root, roundTrip(rootCodec, root))

        // Copy deep-copies nested mutable records: mutating original Address doesn't affect copied Order.
        val copiedOrder = orderCodec.copy(order)
        assertEquals(order, copiedOrder)
        assertNotSame(order, copiedOrder)
        addressClass.getMethod("setZip", Int::class.javaPrimitiveType).invoke(addr1, 99999)
        val copiedShipping = orderClass.getMethod("getShipping").invoke(copiedOrder)
        assertEquals(10115, addressClass.getMethod("getZip").invoke(copiedShipping))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `every allowed row of the persisted-state rule table compiles and round-trips, including explicit-api mode`(
        compiler: KotlinCompiler,
    ) {
        val result = compileSingle(
            """
            package com.example

            import io.flinkt.FlinkType

            public typealias UserIdAlias = Long

            public interface Identifiable {
                public val id: Long
            }

            public abstract class StatelessBase {
                public abstract val id: Long
                public val derivedFromBase: String get() = "id=${'$'}id"
            }

            @FlinkType
            public data class RichRecord(
                public override val id: UserIdAlias,
                public var name: String = "anonymous",
                public val `in`: Int = 0,
            ) : StatelessBase(), Identifiable {
                public val fullLabel: String
                    get() = "${'$'}id:${'$'}name"

                public var computedVar: String
                    get() = name
                    set(v) { name = v }

                public constructor(id: Long) : this(id = id, name = "default", `in` = 1)

                init {
                    require(id >= 0) { "id must be non-negative" }
                }

                public companion object {
                    public const val TAG: String = "rich"
                }
            }

            @FlinkType
            internal data class InternalRecord internal constructor(
                internal val code: Int,
                internal var label: String,
            )
            """,
            compiler = compiler,
            compilerArguments = listOf("-Xexplicit-api=strict"),
        ).assertSuccess()

        val loader = result.classLoader()
        val registry = TypeRegistry.load(loader)

        // RichRecord schema has only its 3 primary-constructor properties (no computed props, no companion const).
        val richClass = loader.loadClass("com.example.RichRecord")
        val richSchema = registry.declaration(richClass)!!.schema as RecordSchema
        assertEquals(listOf("id", "name", "in"), richSchema.fields.map { it.name })
        assertEquals(
            listOf("kotlin.Long", "kotlin.String", "kotlin.Int"),
            richSchema.fields.map { (it.type as SchemaType.ClassType).name },
        )

        val richCodec = codecFor(registry, loader, "com.example.RichRecord")
        val rich = richClass.getConstructor(Long::class.javaPrimitiveType, String::class.java, Int::class.javaPrimitiveType)
            .newInstance(5L, "Ada", 3)
        assertEquals(rich, roundTrip(richCodec, rich))

        // var property causes copy() to allocate an independent copy.
        val copiedRich = richCodec.copy(rich)
        assertEquals(rich, copiedRich)
        assertNotSame(rich, copiedRich)
        richClass.getMethod("setName", String::class.java).invoke(rich, "Changed")
        assertEquals("Ada", richClass.getMethod("getName").invoke(copiedRich))

        // Internal class with internal constructor and internal properties works too.
        val internalClass = loader.loadClass("com.example.InternalRecord")
        val internalCodec = codecFor(registry, loader, "com.example.InternalRecord")
        val internalValue = internalClass.getConstructor(Int::class.javaPrimitiveType, String::class.java).newInstance(7, "ok")
        assertEquals(internalValue, roundTrip(internalCodec, internalValue))
    }

    @Test
    fun `language version 2_3 succeeds with minimum-Kotlin warning, and 2_4 reports no warning`() {
        val code = """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class User(val id: Long, val name: String)
        """
        val at23 = compileSingle(
            code,
            compiler = KotlinCompiler.BUILD,
            compilerArguments = listOf("-language-version", "2.3"),
        ).assertSuccess()
        val kspWarnings23 = at23.warnings.filter { it.origin == Origin.KSP }
        assertEquals(1, kspWarnings23.size, "Expected one KSP warning at language version 2.3: ${at23.warnings}")
        val warningText = kspWarnings23.single().message
        assertTrue("2.3" in warningText, warningText)
        assertTrue("2.4" in warningText, warningText)
        assertTrue("docs/kotlin-compatibility.md" in warningText, warningText)

        for (compiler in KotlinCompiler.entries) {
            val at24 = compileSingle(code, compiler = compiler).assertSuccess()
            assertEquals(emptyList<Any>(), at24.warnings, "Expected no warnings at 2.4 on $compiler: ${at24.warnings}")
        }
    }
}
