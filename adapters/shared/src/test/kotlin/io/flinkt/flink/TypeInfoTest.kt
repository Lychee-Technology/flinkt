package io.flinkt.flink

import io.flinkt.FlinkType
import io.flinkt.codec.CodecFactory
import io.flinkt.codec.GeneratedCodec
import io.flinkt.codec.codec
import io.flinkt.registry.GeneratedTypeModule
import io.flinkt.registry.MutableTypeRegistry
import io.flinkt.registry.TypeRegistry
import io.flinkt.registry.resolve
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.testing.exactTypeOf
import io.flinkt.testing.shouldBe
import io.flinkt.types.BuiltInKind
import io.flinkt.types.UnsupportedTypeException
import java.io.DataInput
import java.io.DataOutput
import java.math.BigDecimal
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.BooleanSerializer
import org.apache.flink.api.common.typeutils.base.ByteSerializer
import org.apache.flink.api.common.typeutils.base.CharSerializer
import org.apache.flink.api.common.typeutils.base.DoubleSerializer
import org.apache.flink.api.common.typeutils.base.FloatSerializer
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.typeutils.base.ShortSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * The TypeInformation contracts of `typeInfo<T>()` (docs/testing.md, "TypeInformation contracts"), on the adapter's
 * own Flink line: a built-in gives Flink's own `BasicTypeInfo` and serializer, and every other type fails explicitly,
 * naming its full Kotlin type.
 */
class TypeInfoTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `each non-null built-in gives the BasicTypeInfo that Flink uses for its boxed JVM class`() {
        assertEquals(BuiltInKind.entries.toSet(), builtIns.map { it.kind }.toSet(), "every built-in kind has a case")
        for (case in builtIns) {
            assertSame(case.flink, case.typeInfo, case.kind.kotlinName)
            assertEquals(BasicTypeInfo.getInfoFor(case.kind.jvmClass), case.typeInfo, case.kind.kotlinName)
            assertEquals(case.kind.jvmClass, case.typeInfo.typeClass, case.kind.kotlinName)
        }
        assertSame(BasicTypeInfo.INT_TYPE_INFO, typeInfo<Int>())
        assertSame(BasicTypeInfo.STRING_TYPE_INFO, typeInfo<String>())
    }

    @Test
    fun `Flink creates its own serializer for each built-in, never a generic one`() {
        // The serializer config of a job that configures nothing, as Flink passes it.
        val config = ExecutionConfig().serializerConfig
        for (case in builtIns) {
            assertFalse(case.typeInfo is GenericTypeInfo<*>, case.kind.kotlinName)
            assertEquals(case.serializer, case.typeInfo.createSerializer(config).javaClass, case.kind.kotlinName)
        }
    }

    @Test
    fun `the result's static type is the TypeInformation of the Kotlin type`() {
        exactTypeOf(typeInfo<Int>()).shouldBe<TypeInformation<Int>>()
        exactTypeOf(typeInfo<String>()).shouldBe<TypeInformation<String>>()
    }

    @Test
    fun `a nullable type fails instead of resolving as the non-null type`() {
        val string = assertThrows<UnsupportedTypeException> { typeInfo<String?>() }
        assertEquals("Flinkt can't resolve kotlin.String?: nullable types aren't supported yet. $FIXES", string.message)
        assertEquals("kotlin.String?", string.canonicalType)

        val int = assertThrows<UnsupportedTypeException> { typeInfo<Int?>() }
        assertEquals("Flinkt can't resolve kotlin.Int?: nullable types aren't supported yet. $FIXES", int.message)
    }

    @Test
    fun `an unmodeled nested generic type fails with its full Kotlin type and both fixes`() {
        val e = assertThrows<UnsupportedTypeException> { typeInfo<Map<UserId, List<Event?>>>() }
        assertEquals(
            "Flinkt can't resolve kotlin.collections.Map<io.flinkt.flink.TypeInfoTest.UserId, " +
                "kotlin.collections.List<io.flinkt.flink.TypeInfoTest.Event?>>: List and Map aren't supported yet. " +
                "To give Flink a TypeInformation for it, annotate the type with @FlinkType, or pass a " +
                "TypeInformation explicitly",
            e.message,
        )
    }

    @Test
    fun `a class that isn't annotated fails with both fixes`() {
        val e = assertThrows<UnsupportedTypeException> { typeInfo<Plain>() }
        assertEquals(
            "Flinkt can't resolve io.flinkt.flink.TypeInfoTest.Plain: it isn't a built-in type, and it isn't " +
                "annotated with @FlinkType. $FIXES",
            e.message,
        )
    }

    @Test
    fun `every other type fails, the types Flink has a BasicTypeInfo for but Flinkt doesn't map included`() {
        val unsupported: Map<String, () -> TypeInformation<*>> = mapOf(
            "kotlin.collections.List<kotlin.String>" to { typeInfo<List<String>>() },
            "kotlin.collections.MutableList<kotlin.String>" to { typeInfo<MutableList<String>>() },
            "kotlin.collections.Set<kotlin.Int>" to { typeInfo<Set<Int>>() },
            "kotlin.IntArray" to { typeInfo<IntArray>() },
            "kotlin.Array<kotlin.String>" to { typeInfo<Array<String>>() },
            "kotlin.Any" to { typeInfo<Any>() },
            "kotlin.Unit" to { typeInfo<Unit>() },
            // Each of these would fix a state format, so each needs its own change with its reason.
            "java.math.BigDecimal" to { typeInfo<BigDecimal>() },
            "java.time.Instant" to { typeInfo<Instant>() },
            "java.util.Date" to { typeInfo<Date>() },
        )
        for ((type, call) in unsupported) {
            val e = assertThrows<UnsupportedTypeException>(type) { call() }
            assertEquals(type, e.canonicalType)
            val message = checkNotNull(e.message)
            assertTrue(message.startsWith("Flinkt can't resolve $type: ") && message.endsWith(". $FIXES")) { message }
        }
    }

    @Test
    fun `a type inferred from a Java method that returns String resolves as String`() {
        val version = System.getProperty("java.version")
        // The inferred type is the platform type String!, which isn't the KType of String.
        assertNotEquals(typeOf<String>(), kTypeOf(version))
        assertSame(BasicTypeInfo.STRING_TYPE_INFO, typeInfoOf(version))
    }

    @Test
    fun `the tests run without kotlin-reflect on the runtime classpath`() {
        assertThrows<ClassNotFoundException> { Class.forName("kotlin.reflect.jvm.internal.KClassImpl") }
        assertThrows<ClassNotFoundException> { Class.forName("kotlin.reflect.full.KClasses") }
        // Without kotlin-reflect, typeOf gives the standard library's own KType.
        assertEquals("kotlin.jvm.internal.TypeReference", typeOf<Map<String, List<Int?>>>().javaClass.name)
    }

    @Test
    fun `a generated type that a module on the context class loader registers fails with both fixes`() {
        val loader = withServices(RegisteredTypes::class.java.name)

        val e = withContextClassLoader(loader) { assertThrows<UnsupportedTypeException> { typeInfo<Registered>() } }
        assertEquals(
            "Flinkt can't resolve io.flinkt.flink.TypeInfoTest.Registered: it's a generated type, and typeInfo() has " +
                "no TypeInformation for a generated type yet. Of the two fixes, annotating the type with @FlinkType " +
                "or passing a TypeInformation explicitly, the annotation is already there and gives it no " +
                "TypeInformation in this release, so pass a TypeInformation explicitly",
            e.message,
        )
        assertEquals("io.flinkt.flink.TypeInfoTest.Registered", e.canonicalType)
        // A nullable generated type gets the same message, not the nullable one, whose first fix it already has.
        val nullable = withContextClassLoader(loader) {
            assertThrows<UnsupportedTypeException> { typeInfo<Registered?>() }
        }
        assertEquals(generatedMessage("io.flinkt.flink.TypeInfoTest.Registered?"), nullable.message)
        // A registry with generated types still gives Flink's own TypeInformation for a built-in.
        assertSame(BasicTypeInfo.LONG_TYPE_INFO, withContextClassLoader(loader) { typeInfo<Long>() })
    }

    @Test
    fun `a generated type fails before its module's codec factory runs`() {
        val loader = withServices(FactoryTypes::class.java.name)
        factoryRuns.set(0)

        // Its factory asks for the codec of a nullable type, which would fail with that nested type's reason.
        val nested = withContextClassLoader(loader) {
            assertThrows<UnsupportedTypeException> { typeInfo<NestedNullable>() }
        }
        assertEquals(generatedMessage("io.flinkt.flink.TypeInfoTest.NestedNullable"), nested.message)
        // Its factory throws an IllegalStateException, which would escape typeInfo() as it is.
        val failing = withContextClassLoader(loader) {
            assertThrows<UnsupportedTypeException> { typeInfo<FailingFactory>() }
        }
        assertEquals(generatedMessage("io.flinkt.flink.TypeInfoTest.FailingFactory"), failing.message)
        assertEquals(0, factoryRuns.get(), "no codec factory ran")

        // The control: core's resolution of the same type runs its factory, and the factory's exception escapes.
        val control = assertThrows<IllegalStateException> { TypeRegistry.of(FactoryTypes()).resolve<FailingFactory>() }
        assertEquals("FailingFactory's codec factory ran", control.message)
        assertEquals(1, factoryRuns.get())
    }

    @Test
    fun `an annotated type that no module on the context class loader registers fails with the reason`() {
        val e = assertThrows<UnsupportedTypeException> { typeInfo<Registered>() }
        assertEquals(
            "Flinkt can't resolve io.flinkt.flink.TypeInfoTest.Registered: it's annotated with @FlinkType, but no " +
                "GeneratedTypeModule that this registry found registers it. Check that flinkt-ksp runs in the module " +
                "that declares it. $FIXES",
            e.message,
        )
    }

    @Test
    fun `a thread without a context class loader resolves a built-in`() {
        assertSame(BasicTypeInfo.INT_TYPE_INFO, withContextClassLoader(null) { typeInfo<Int>() })
    }

    @JvmInline
    value class UserId(val value: Long)

    class Event

    class Plain

    @FlinkType
    data class Registered(val id: Long)

    /** A stand-in for the module flinkt-ksp is to generate, registering [Registered]. */
    class RegisteredTypes : GeneratedTypeModule {
        override fun register(registry: MutableTypeRegistry) {
            val schema = RecordSchema(Registered::class.java.name, listOf(FieldSchema("id", SchemaType.ClassType("kotlin.Long"))))
            registry.register(Registered::class, schema, CodecFactory { RegisteredCodec })
        }
    }

    object RegisteredCodec : GeneratedCodec<Registered> {
        override fun serialize(value: Registered, output: DataOutput) = output.writeLong(value.id)

        override fun deserialize(input: DataInput): Registered = Registered(input.readLong())

        override fun copy(value: Registered): Registered = value
    }

    @FlinkType
    data class NestedNullable(val id: Long?)

    @FlinkType
    data class FailingFactory(val id: Long)

    /**
     * Registers two records whose codec factories count their runs in [factoryRuns] and fail: [NestedNullable]'s asks
     * for the codec of a nullable type, and [FailingFactory]'s throws an `IllegalStateException`.
     */
    class FactoryTypes : GeneratedTypeModule {
        override fun register(registry: MutableTypeRegistry) {
            registry.register(
                NestedNullable::class,
                RecordSchema(
                    NestedNullable::class.java.name,
                    listOf(FieldSchema("id", SchemaType.ClassType("kotlin.Long", isNullable = true))),
                ),
                CodecFactory { context ->
                    factoryRuns.incrementAndGet()
                    context.codec<Long?>()
                    error("unreachable: a nullable type has no codec")
                },
            )
            registry.register(
                FailingFactory::class,
                RecordSchema(
                    FailingFactory::class.java.name,
                    listOf(FieldSchema("id", SchemaType.ClassType("kotlin.Long"))),
                ),
                CodecFactory {
                    factoryRuns.incrementAndGet()
                    error("FailingFactory's codec factory ran")
                },
            )
        }
    }

    /** The message of a generated [type], which gives both fixes and says the annotation is already there. */
    private fun generatedMessage(type: String): String =
        "Flinkt can't resolve $type: it's a generated type, and typeInfo() has no TypeInformation for a generated " +
            "type yet. $GENERATED_FIXES"

    /** A class loader that lists [providers] as `GeneratedTypeModule`s, over the test's own. */
    private fun withServices(vararg providers: String): ClassLoader {
        val file = dir.resolve("META-INF/services/${GeneratedTypeModule::class.java.name}")
        Files.createDirectories(file.parent)
        Files.write(file, providers.joinToString("\n", postfix = "\n").toByteArray())
        return URLClassLoader(arrayOf(dir.toUri().toURL()), javaClass.classLoader)
    }

    private fun <R> withContextClassLoader(loader: ClassLoader?, block: () -> R): R {
        val thread = Thread.currentThread()
        val previous = thread.contextClassLoader
        thread.contextClassLoader = loader
        try {
            return block()
        } finally {
            thread.contextClassLoader = previous
        }
    }

    /** `typeInfo<T>()` with [T] inferred from [value]'s static type, as a view's operator infers its result type. */
    private inline fun <reified T> typeInfoOf(@Suppress("UNUSED_PARAMETER") value: T): TypeInformation<T> = typeInfo()

    /** The `KType` of [value]'s static type. A value from a Java method gives a platform type. */
    private inline fun <reified T> kTypeOf(@Suppress("UNUSED_PARAMETER") value: T): KType = typeOf<T>()

    /** A built-in, the `typeInfo` of its Kotlin type, and what Flink has for its boxed JVM class. */
    class BuiltIn(
        val kind: BuiltInKind,
        val typeInfo: TypeInformation<*>,
        val flink: BasicTypeInfo<*>,
        val serializer: Class<out TypeSerializer<*>>,
    )

    companion object {
        /** The fixes every unsupported type's message ends with. */
        const val FIXES: String =
            "To give Flink a TypeInformation for it, annotate the type with @FlinkType, or pass a TypeInformation explicitly"

        /** The fixes a generated type's message ends with: both, and which one works for it in this release. */
        const val GENERATED_FIXES: String =
            "Of the two fixes, annotating the type with @FlinkType or passing a TypeInformation explicitly, the " +
                "annotation is already there and gives it no TypeInformation in this release, so pass a " +
                "TypeInformation explicitly"

        /** How many times [FactoryTypes]' codec factories ran. */
        val factoryRuns: AtomicInteger = AtomicInteger()

        val builtIns: List<BuiltIn> = listOf(
            BuiltIn(BuiltInKind.BOOLEAN, typeInfo<Boolean>(), BasicTypeInfo.BOOLEAN_TYPE_INFO, BooleanSerializer::class.java),
            BuiltIn(BuiltInKind.BYTE, typeInfo<Byte>(), BasicTypeInfo.BYTE_TYPE_INFO, ByteSerializer::class.java),
            BuiltIn(BuiltInKind.SHORT, typeInfo<Short>(), BasicTypeInfo.SHORT_TYPE_INFO, ShortSerializer::class.java),
            BuiltIn(BuiltInKind.INT, typeInfo<Int>(), BasicTypeInfo.INT_TYPE_INFO, IntSerializer::class.java),
            BuiltIn(BuiltInKind.LONG, typeInfo<Long>(), BasicTypeInfo.LONG_TYPE_INFO, LongSerializer::class.java),
            BuiltIn(BuiltInKind.FLOAT, typeInfo<Float>(), BasicTypeInfo.FLOAT_TYPE_INFO, FloatSerializer::class.java),
            BuiltIn(BuiltInKind.DOUBLE, typeInfo<Double>(), BasicTypeInfo.DOUBLE_TYPE_INFO, DoubleSerializer::class.java),
            BuiltIn(BuiltInKind.CHAR, typeInfo<Char>(), BasicTypeInfo.CHAR_TYPE_INFO, CharSerializer::class.java),
            BuiltIn(BuiltInKind.STRING, typeInfo<String>(), BasicTypeInfo.STRING_TYPE_INFO, StringSerializer::class.java),
        )
    }
}
