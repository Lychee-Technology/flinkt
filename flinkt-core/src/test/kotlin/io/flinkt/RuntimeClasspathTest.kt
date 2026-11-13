package io.flinkt

import io.flinkt.example.StandInTypes
import io.flinkt.example.User
import io.flinkt.example.UserCodec
import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.registry.resolve
import java.lang.annotation.RetentionPolicy
import kotlin.reflect.typeOf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/** The tests run, and resolution works, with only kotlin-stdlib on the runtime classpath. */
class RuntimeClasspathTest {
    @Test
    fun `kotlin-reflect isn't on the runtime classpath`() {
        assertThrows<ClassNotFoundException> { Class.forName("kotlin.reflect.jvm.internal.KClassImpl") }
        assertThrows<ClassNotFoundException> { Class.forName("kotlin.reflect.full.KClasses") }
        // Without kotlin-reflect, typeOf gives the standard library's own KType.
        assertEquals("kotlin.jvm.internal.TypeReference", typeOf<List<User?>>().javaClass.name)
        val registry = TypeRegistry.of(StandInTypes())
        assertSame(UserCodec, assertInstanceOf(Resolution.Codec::class.java, registry.resolve<User>()).codec)
        assertInstanceOf(Resolution.BuiltIn::class.java, registry.resolve<String>())
    }

    @Test
    fun `FlinkType is kept at runtime, so a consumer's KSP and type resolution can read it from a jar`() {
        val retention = FlinkType::class.java.getAnnotation(java.lang.annotation.Retention::class.java)
        assertEquals(RetentionPolicy.RUNTIME, retention.value)
        assertEquals(true, User::class.java.isAnnotationPresent(FlinkType::class.java))
    }
}
