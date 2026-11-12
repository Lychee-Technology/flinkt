package io.flinkt

import io.flinkt.testing.ClassFiles
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class PublishedClassesTest {
    @Test
    fun `classes target Java 8 and carry Kotlin metadata 2_4_0`() {
        assertEquals(52, ClassFiles.majorVersion(FlinkType::class.java))
        assertEquals("2.4.0", ClassFiles.kotlinMetadataVersion(FlinkType::class.java))
    }

    @ParameterizedTest
    @EnumSource(KotlinCompiler::class)
    fun `user code compiles against flinkt-core`(compiler: KotlinCompiler) {
        KotlinCompilation(compiler)
            .compile(
                """
                import io.flinkt.FlinkType

                @FlinkType
                data class User(val id: Long, val name: String)
                """,
            )
            .assertSuccess()
    }
}
