package io.flinkt.registry

import io.flinkt.example.IntCodec
import io.flinkt.example.StandInBuiltIns
import io.flinkt.example.StandInTypes
import io.flinkt.example.StringCodec
import io.flinkt.example.User
import io.flinkt.example.UserCodec
import io.flinkt.example.UserCodecFactory
import io.flinkt.example.userSchema
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.types.UnsupportedTypeException
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TypeRegistryTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `modules listed in META-INF services are discovered through the supplied class loader`() {
        val loader = withServices(StandInTypes::class.java.name, StandInBuiltIns::class.java.name)

        val registry = TypeRegistry.load(loader)

        assertEquals(listOf(StandInTypes::class.java, StandInBuiltIns::class.java), registry.modules.map { it.javaClass })
        assertSame(UserCodec, registry.codec<User>())
        assertSame(IntCodec, registry.codec<Int>())
        // The test's own class loader lists no module, so its registry has no types.
        assertEquals(emptyList<GeneratedTypeModule>(), TypeRegistry.load(javaClass.classLoader).modules)
        assertNull(TypeRegistry.load(javaClass.classLoader).declaration(User::class))
    }

    @Test
    fun `lookup is by type and gives the registered schema, factory and module`() {
        val module = StandInTypes()
        val declaration = TypeRegistry.of(module).declaration(User::class)!!

        assertSame(User::class.java, declaration.type.java)
        assertEquals(userSchema, declaration.schema)
        assertSame(UserCodecFactory, declaration.codecFactory)
        assertSame(module, declaration.module)
        assertSame(User::class.java, TypeRegistry.of(module).declaration(User::class.java)!!.type.java)
    }

    @Test
    fun `a type registered by two modules fails with both modules named`() {
        val error = assertThrows<IllegalStateException> { TypeRegistry.of(StandInTypes(), DuplicateUser()) }

        assertContains(error.message, "${User::class.java.name} is registered twice: by ${StandInTypes::class.java.name} (")
        assertContains(error.message, " and by ${DuplicateUser::class.java.name} (")
    }

    @Test
    fun `a duplicate found through ServiceLoader names both modules too`() {
        val loader = withServices(StandInTypes::class.java.name, DuplicateUser::class.java.name)

        val error = assertThrows<IllegalStateException> { TypeRegistry.load(loader) }

        assertContains(error.message, "by ${StandInTypes::class.java.name}")
        assertContains(error.message, "by ${DuplicateUser::class.java.name}")
    }

    @Test
    fun `a type registered twice by one module, and a built-in registered twice, fail`() {
        val twice = object : GeneratedTypeModule {
            override fun register(registry: MutableTypeRegistry) {
                repeat(2) { registry.register(User::class, userSchema, UserCodecFactory) }
            }
        }
        assertContains(
            assertThrows<IllegalStateException> { TypeRegistry.of(twice) }.message,
            "${User::class.java.name} is registered twice by ",
        )
        assertContains(
            assertThrows<IllegalStateException> { TypeRegistry.of(StandInBuiltIns(), StandInBuiltIns()) }.message,
            "the built-in type kotlin.Int is registered twice: by ${StandInBuiltIns::class.java.name}",
        )
    }

    @Test
    fun `a schema must name the class it's registered for`() {
        val error = assertThrows<IllegalStateException> {
            TypeRegistry.of(
                module { registry ->
                    registry.register(User::class, RecordSchema("com.example.User", emptyList()), UserCodecFactory)
                },
            )
        }
        assertContains(error.message, "registers ${User::class.java.name} with the schema of com.example.User")
    }

    @Test
    fun `a built-in type can't be registered as a generated declaration`() {
        val error = assertThrows<IllegalStateException> {
            TypeRegistry.of(
                module { registry ->
                    registry.register(String::class, RecordSchema("java.lang.String", emptyList())) { error("unused") }
                },
            )
        }
        assertContains(error.message, "registers the built-in type java.lang.String as a generated declaration")
    }

    @Test
    fun `a built-in codec is registered for its Kotlin class and resolves only for that type`() {
        val strings = TypeRegistry.of(module { registry -> registry.registerBuiltIn(String::class) { StringCodec } })

        assertSame(StringCodec, strings.codec<String>())
        assertSame(IntCodec, TypeRegistry.of(module { it.registerBuiltIn(Int::class) { IntCodec } }).codec<Int>())
        val error = assertThrows<UnsupportedTypeException> { strings.codec<Int>() }
        assertEquals("kotlin.Int", error.canonicalType)
    }

    @Test
    fun `a class that isn't a built-in type can't be registered as one`() {
        val error = assertThrows<IllegalStateException> {
            TypeRegistry.of(module { registry -> registry.registerBuiltIn(User::class, UserCodecFactory) })
        }
        assertContains(error.message, "registers a built-in codec for ${User::class.java.name}, which isn't a built-in type")
        assertContains(error.message, "kotlin.Int, kotlin.Long")
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `a built-in codec for another type doesn't compile`(compiler: KotlinCompiler) {
        fun registering(call: String) = """
            import io.flinkt.codec.CodecFactory
            import io.flinkt.codec.GeneratedCodec
            import io.flinkt.registry.GeneratedTypeModule
            import io.flinkt.registry.MutableTypeRegistry

            class BuiltIns(private val strings: CodecFactory<String>, private val string: GeneratedCodec<String>) : GeneratedTypeModule {
                override fun register(registry: MutableTypeRegistry) {
                    $call
                }
            }
            """

        KotlinCompilation(compiler).compile(registering("registry.registerBuiltIn(String::class, strings)")).assertSuccess()
        KotlinCompilation(compiler).compile(registering("registry.registerBuiltIn(Int::class, strings)"))
            .assertFailure("ARGUMENT_TYPE_MISMATCH", "'CodecFactory<String>', but 'CodecFactory<Int>' was expected")
        KotlinCompilation(compiler).compile(registering("registry.registerBuiltIn(Int::class) { string }"))
            .assertFailure("RETURN_TYPE_MISMATCH", "expected 'GeneratedCodec<Int>', actual 'GeneratedCodec<String>'")
    }

    @Test
    fun `a module can't register after its register call returns`() {
        lateinit var kept: MutableTypeRegistry
        TypeRegistry.of(module { kept = it })

        val error = assertThrows<IllegalStateException> { kept.registerBuiltIn(Int::class) { IntCodec } }
        assertContains(error.message, "registered a type after its register() returned")
    }

    @Test
    fun `a module that fails to register is named`() {
        val error = assertThrows<IllegalStateException> {
            TypeRegistry.of(
                module { registry ->
                    registry.register(
                        User::class,
                        RecordSchema(User::class.java.name, listOf(FieldSchema("id", LONG), FieldSchema("id", LONG))),
                        UserCodecFactory,
                    )
                },
            )
        }
        assertContains(error.message, "failed to register its types: ${User::class.java.name} has two fields named id")
        assertInstanceOf(IllegalArgumentException::class.java, error.cause)
    }

    @Test
    fun `a module that can't be loaded fails the registry`() {
        val error = assertThrows<IllegalStateException> { TypeRegistry.load(withServices("com.example.Missing")) }
        assertContains(error.message, "A GeneratedTypeModule couldn't be loaded")
        assertContains(error.message, "com.example.Missing")
    }

    /** A class loader that lists [providers] as `GeneratedTypeModule`s and loads everything else from the test. */
    private fun withServices(vararg providers: String): ClassLoader {
        val file = dir.resolve("META-INF/services/${GeneratedTypeModule::class.java.name}")
        Files.createDirectories(file.parent)
        Files.write(file, providers.joinToString("\n", postfix = "\n").toByteArray())
        return URLClassLoader(arrayOf(dir.toUri().toURL()), javaClass.classLoader)
    }

    private fun module(body: (MutableTypeRegistry) -> Unit) = object : GeneratedTypeModule {
        override fun register(registry: MutableTypeRegistry) = body(registry)
    }

    /** A second module that registers [User], as if KSP had generated it again in a consumer module. */
    class DuplicateUser : GeneratedTypeModule {
        override fun register(registry: MutableTypeRegistry) {
            registry.register(User::class, userSchema, UserCodecFactory)
        }
    }

    private companion object {
        val LONG = SchemaType.ClassType("kotlin.Long")
    }
}
