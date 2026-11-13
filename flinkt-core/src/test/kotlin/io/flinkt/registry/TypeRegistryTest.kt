package io.flinkt.registry

import io.flinkt.example.StandInTypes
import io.flinkt.example.User
import io.flinkt.example.UserCodec
import io.flinkt.example.UserCodecFactory
import io.flinkt.example.userSchema
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.types.BuiltInKind
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
        val loader = withServices(StandInTypes::class.java.name)

        val registry = TypeRegistry.load(loader)

        assertEquals(listOf(StandInTypes::class.java), registry.modules.map { it.javaClass })
        assertSame(UserCodec, registry.codecOf<User>())
        // The stand-in module registers generated records only, and a built-in resolves to its kind all the same.
        assertEquals(BuiltInKind.INT, assertInstanceOf(Resolution.BuiltIn::class.java, registry.resolve<Int>()).kind)
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
    fun `a type registered twice by one module fails`() {
        val twice = object : GeneratedTypeModule {
            override fun register(registry: MutableTypeRegistry) {
                repeat(2) { registry.register(User::class, userSchema, UserCodecFactory) }
            }
        }
        assertContains(
            assertThrows<IllegalStateException> { TypeRegistry.of(twice) }.message,
            "${User::class.java.name} is registered twice by ",
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
        fun assertRejected(name: String, register: (MutableTypeRegistry) -> Unit) {
            val error = assertThrows<IllegalStateException> { TypeRegistry.of(module(register)) }
            assertContains(error.message, "registers the built-in type $name as a generated declaration")
            assertContains(error.message, "A built-in type resolves to its built-in kind, and no module defines its codec")
        }

        assertRejected("java.lang.String") { it.register(String::class, RecordSchema("java.lang.String", emptyList())) { error("unused") } }
        assertRejected("int") { it.register(Int::class, RecordSchema("int", emptyList())) { error("unused") } }
        assertRejected("long") { it.register(Long::class, RecordSchema("long", emptyList())) { error("unused") } }
    }

    @Test
    fun `a module registers generated declarations only`() {
        // A built-in's bytes are Flink's, so no jar on the classpath can define them through the SPI.
        assertEquals(listOf("register"), MutableTypeRegistry::class.java.methods.map { it.name })
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `registering a built-in codec doesn't compile`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(
                """
                import io.flinkt.codec.CodecFactory
                import io.flinkt.registry.GeneratedTypeModule
                import io.flinkt.registry.MutableTypeRegistry

                class BuiltIns(private val ints: CodecFactory<Int>) : GeneratedTypeModule {
                    override fun register(registry: MutableTypeRegistry) {
                        registry.registerBuiltIn(Int::class, ints)
                    }
                }
                """,
            )
            .assertFailure("UNRESOLVED_REFERENCE", "registerBuiltIn")
    }

    @Test
    fun `a module can't register after its register call returns`() {
        lateinit var kept: MutableTypeRegistry
        TypeRegistry.of(module { kept = it })

        val error = assertThrows<IllegalStateException> { kept.register(User::class, userSchema, UserCodecFactory) }
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
