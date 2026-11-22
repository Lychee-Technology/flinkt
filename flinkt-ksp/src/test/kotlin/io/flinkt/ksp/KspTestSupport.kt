package io.flinkt.ksp

import io.flinkt.codec.GeneratedCodec
import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.testing.compile.CompilationResult
import io.flinkt.testing.compile.KotlinCompilation
import io.flinkt.testing.compile.KotlinCompiler
import io.flinkt.testing.compile.SourceFile
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import kotlin.jvm.internal.Reflection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf

internal object KspTestSupport {
    val processorClasspath: List<File> =
        System.getProperty("flinkt.test.kspProcessorClasspath").split(File.pathSeparatorChar).map(::File)

    fun compile(
        compiler: KotlinCompiler = KotlinCompiler.BUILD,
        vararg sources: SourceFile,
        extraClasspath: List<File> = emptyList(),
        compilerArguments: List<String> = emptyList(),
    ): CompilationResult {
        val base = KotlinCompilation(compiler)
        return KotlinCompilation(
            compiler = compiler,
            classpath = base.classpath + extraClasspath,
            kspProcessorClasspath = processorClasspath,
            compilerArguments = compilerArguments,
        ).compile(*sources)
    }

    fun compileSingle(
        code: String,
        compiler: KotlinCompiler = KotlinCompiler.BUILD,
        path: String = "com/example/Types.kt",
        compilerArguments: List<String> = emptyList(),
    ): CompilationResult = compile(
        compiler = compiler,
        SourceFile(path, code.trimIndent() + "\n"),
        compilerArguments = compilerArguments,
    )

    @Suppress("UNCHECKED_CAST")
    fun codecFor(registry: TypeRegistry, loader: ClassLoader, binaryName: String): GeneratedCodec<Any> {
        val clazz = loader.loadClass(binaryName)
        return assertInstanceOf(
            Resolution.Codec::class.java,
            registry.resolve(Reflection.typeOf(clazz)),
        ).codec as GeneratedCodec<Any>
    }

    fun serialize(codec: GeneratedCodec<Any>, value: Any): ByteArray =
        ByteArrayOutputStream().also { codec.serialize(value, DataOutputStream(it)) }.toByteArray()

    fun deserialize(codec: GeneratedCodec<Any>, bytes: ByteArray): Any {
        val input = DataInputStream(ByteArrayInputStream(bytes))
        return codec.deserialize(input).also { assertEquals(-1, input.read(), "unread trailing bytes") }
    }

    fun roundTrip(codec: GeneratedCodec<Any>, value: Any): Any = deserialize(codec, serialize(codec, value))

    fun hex(bytes: ByteArray): String = bytes.joinToString(" ") { "%02x".format(it.toInt() and 0xff) }
}
