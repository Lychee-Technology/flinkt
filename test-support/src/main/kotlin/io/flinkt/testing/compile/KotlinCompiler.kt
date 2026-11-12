package io.flinkt.testing.compile

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.net.URLClassLoader
import java.util.concurrent.ConcurrentHashMap

/**
 * A Kotlin compiler pinned in `gradle/libs.versions.toml` (docs/kotlin-compatibility.md, "What CI tests").
 *
 * Each runs in the test JVM, in its own class loader, so a test can compile with either one regardless of the
 * Kotlin that built the test.
 */
public enum class KotlinCompiler(private val key: String) {
    /** The build compiler. It builds Flinkt, and every suite runs on it. */
    BUILD("build"),

    /** The minimum supported compiler, 2.4.0. It compiles the code that stands for a user's against Flinkt as built. */
    MINIMUM("minimum"),
    ;

    /** The compiler's version, such as `2.4.0`. */
    public val version: String
        get() = HarnessProperties.string("flinkt.compileTest.kotlin.$key.version")

    /** The language version a compilation with this compiler defaults to, such as `2.4`. */
    public val languageVersion: String
        get() = version.split('.').take(2).joinToString(".")

    internal fun run(arguments: List<String>): CompilerRun = loaded.computeIfAbsent(this) { compiler ->
        IsolatedCompiler(compiler.version, HarnessProperties.files("flinkt.compileTest.kotlin.$key.classpath"))
    }.run(arguments)

    private companion object {
        val loaded = ConcurrentHashMap<KotlinCompiler, IsolatedCompiler>()
    }
}

internal class CompilerRun(val exitCode: String, val output: String)

/** One compiler version, loaded once per test JVM and isolated from the test's own Kotlin. */
private class IsolatedCompiler(expectedVersion: String, classpath: List<File>) {
    private val loader = URLClassLoader(classpath.map { it.toURI().toURL() }.toTypedArray(), ClassLoader.getPlatformClassLoader())
    private val compilerClass = Class.forName("org.jetbrains.kotlin.cli.jvm.K2JVMCompiler", true, loader)
    private val exec: Method = compilerClass.getMethod("execFullPathsInMessages", PrintStream::class.java, Array<String>::class.java)

    init {
        // Guards against a classpath that dependency resolution moved to another compiler version.
        val loadedVersion = Class.forName("org.jetbrains.kotlin.config.KotlinCompilerVersion", true, loader)
            .getField("VERSION").get(null)
        check(loadedVersion == expectedVersion) {
            "The compile-test classpath for Kotlin $expectedVersion loaded Kotlin $loadedVersion"
        }
    }

    fun run(arguments: List<String>): CompilerRun = synchronized(lock) {
        val buffer = ByteArrayOutputStream()
        val thread = Thread.currentThread()
        val previous = thread.contextClassLoader
        thread.contextClassLoader = loader
        val exitCode = try {
            PrintStream(buffer, true, "UTF-8").use { stream ->
                exec.invoke(compilerClass.getDeclaredConstructor().newInstance(), stream, arguments.toTypedArray())
            }
        } catch (e: InvocationTargetException) {
            throw e.targetException
        } finally {
            thread.contextClassLoader = previous
        }
        CompilerRun(exitCode.toString(), buffer.toString("UTF-8"))
    }

    private companion object {
        // One compilation at a time per JVM: the compiler's environment isn't documented as reentrant.
        val lock = Any()
    }
}
