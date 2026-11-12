package io.flinkt

import io.flinkt.example.StandInBuiltIns
import io.flinkt.example.StandInTypes
import io.flinkt.example.User
import io.flinkt.registry.TypeRegistry
import io.flinkt.registry.codec
import java.lang.annotation.RetentionPolicy
import kotlin.reflect.typeOf
import org.junit.jupiter.api.Assertions.assertEquals
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
        TypeRegistry.of(StandInTypes(), StandInBuiltIns()).codec<User>()
    }

    @Test
    fun `FlinkType is kept at runtime, so a consumer's KSP and type resolution can read it from a jar`() {
        val retention = FlinkType::class.java.getAnnotation(java.lang.annotation.Retention::class.java)
        assertEquals(RetentionPolicy.RUNTIME, retention.value)
        assertEquals(true, User::class.java.isAnnotationPresent(FlinkType::class.java))
    }
}
