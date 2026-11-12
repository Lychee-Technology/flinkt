package io.flinkt.registry

import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.types.TypeShape
import io.flinkt.types.UnsupportedTypeException
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.net.URLClassLoader
import kotlin.reflect.KType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows

/**
 * Two class loaders that each define `com.example.User`, its codec and its module, as two Flink jobs in one JVM do
 * with their user-code class loaders. The classes are compiled once and loaded twice, so the two `User` classes have
 * the same name and bytes and are still two types.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClassLoaderIsolationTest {
    private lateinit var classes: File

    @BeforeAll
    fun compile() {
        val result = KotlinCompilation().compile(STAND_IN).assertSuccess()
        classes = result.classesDir
        File(classes, "META-INF/services/${GeneratedTypeModule::class.java.name}")
            .apply { parentFile.mkdirs() }
            .writeText("com.example.ModelTypes\n")
    }

    @Test
    fun `each class loader gets its own registry, and a lookup through one never returns the other's codec`() {
        val first = Job(newLoader())
        val second = Job(newLoader())
        assertEquals(first.user.name, second.user.name)
        assertNotSame(first.user, second.user)

        // Each registry has its own User, registered by its own module.
        assertSame(first.user, first.registry.declarations.single().type.java)
        assertSame(second.user, second.registry.declarations.single().type.java)
        assertSame(first.loader, first.registry.modules.single().javaClass.classLoader)
        assertNull(first.registry.declaration(second.user))
        assertNull(second.registry.declaration(first.user))

        // Each resolves its own User to a codec from its own class loader, which writes and reads its own User.
        val firstCodec = first.registry.codec(first.userType)
        val secondCodec = second.registry.codec(second.userType)
        assertSame(first.loader, firstCodec.javaClass.classLoader)
        assertSame(second.loader, secondCodec.javaClass.classLoader)
        @Suppress("UNCHECKED_CAST")
        val decoded = roundTrip(firstCodec as io.flinkt.codec.GeneratedCodec<Any>, first.sample)
        assertSame(first.user, decoded.javaClass)

        // The other job's User is the same name and a different type: modeled by its classifier only, and not
        // resolved, with a message that says why.
        assertEquals(TypeShape.ClassifierOnly, first.registry.model(second.userType).shape)
        val error = assertThrows<UnsupportedTypeException> { first.registry.codec(second.userType) }
        assertEquals("com.example.User", error.canonicalType)
        assertContains(error.message, "this registry has a class of that name from another class loader")

        // The other registry's model of its User carries that registry's schema, and still isn't this registry's.
        val foreign = second.registry.model(second.userType)
        assertEquals(TypeShape.Generated(second.registry.declarations.single().schema), foreign.shape)
        val transferred = assertThrows<UnsupportedTypeException> { first.registry.codec(foreign) }
        assertContains(transferred.message, "this registry has a class of that name from another class loader")
    }

    @Test
    fun `discovery finds modules only through the supplied class loader`() {
        assertEquals(emptyList<GeneratedTypeModule>(), TypeRegistry.load(javaClass.classLoader).modules)
        assertEquals(listOf("com.example.ModelTypes"), TypeRegistry.load(newLoader()).modules.map { it.javaClass.name })
    }

    private fun newLoader(): ClassLoader = URLClassLoader(arrayOf(classes.toURI().toURL()), javaClass.classLoader)

    /** What one job's class loader holds: its registry, its User class, User's `KType`, and a User value. */
    private class Job(val loader: ClassLoader) {
        val registry = TypeRegistry.load(loader)
        val user: Class<*> = loader.loadClass("com.example.User")

        @Suppress("UNCHECKED_CAST")
        private val probe = loader.loadClass("com.example.Probe").getDeclaredConstructor().newInstance() as () -> Pair<KType, Any>
        val userType: KType = probe().first
        val sample: Any = probe().second
    }

    private fun <T> roundTrip(codec: io.flinkt.codec.GeneratedCodec<T>, value: T): T {
        val bytes = ByteArrayOutputStream().also { codec.serialize(value, DataOutputStream(it)) }.toByteArray()
        return codec.deserialize(DataInputStream(ByteArrayInputStream(bytes)))
    }

    private companion object {
        val STAND_IN = """
            package com.example

            import io.flinkt.FlinkType
            import io.flinkt.codec.GeneratedCodec
            import io.flinkt.registry.GeneratedTypeModule
            import io.flinkt.registry.MutableTypeRegistry
            import io.flinkt.schema.FieldSchema
            import io.flinkt.schema.RecordSchema
            import io.flinkt.schema.SchemaType
            import java.io.DataInput
            import java.io.DataOutput
            import kotlin.reflect.KType
            import kotlin.reflect.typeOf

            @FlinkType
            data class User(val id: Long, val name: String)

            class UserCodec : GeneratedCodec<User> {
                override fun serialize(value: User, output: DataOutput) {
                    output.writeLong(value.id)
                    output.writeUTF(value.name)
                }

                override fun deserialize(input: DataInput): User = User(input.readLong(), input.readUTF())

                override fun copy(value: User): User = value
            }

            class ModelTypes : GeneratedTypeModule {
                override fun register(registry: MutableTypeRegistry) {
                    val schema = RecordSchema(
                        User::class.java.name,
                        listOf(FieldSchema("id", SchemaType.ClassType("kotlin.Long")), FieldSchema("name", SchemaType.ClassType("kotlin.String"))),
                    )
                    registry.register(User::class, schema) { UserCodec() }
                }
            }

            /** The test can't name this class loader's User, so this gives it User's KType and a value. */
            class Probe : () -> Pair<KType, Any> {
                override fun invoke(): Pair<KType, Any> = typeOf<User>() to User(1, "Ada")
            }
            """
    }
}
