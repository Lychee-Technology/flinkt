package io.flinkt.codegen

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import javax.tools.ToolProvider
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * The extraction on classes that javac compiles here, with and without debug information. The adapters' checkFlinkApi
 * runs it on Flink's own jars.
 */
class FlinkApiExtractionTest {
    @TempDir
    lateinit var dir: File

    @Test
    fun `names come from the LocalVariableTable of a class compiled with debug information`() {
        val api = FlinkApiExtraction.extract(listOf(compile("-g")), listOf("sample.Stream"), "1.0.0")
        assertEquals("1.0.0", api.flinkVersion)
        assertEquals(
            listOf(
                "sample.Base.accept(Ljava/lang/Object;)V [value]",
                // A long or a double takes two slots, which the names of the parameters after it skip.
                "sample.Base.mixed(JLjava/lang/String;DI)V [first, second, third, fourth]",
                "sample.Base.self()Lsample/Base; []",
                "sample.Base.union([Lsample/Base;)Lsample/Base; [streams]",
                "sample.Named.describe(Ljava/lang/String;)Ljava/lang/String; [prefix]",
                "sample.Stream.accept(Ljava/lang/String;)V [value]",
                "sample.Stream.label(Ljava/lang/String;)Ljava/lang/String; [prefix]",
            ),
            api.methods.map { "${it.className}.${it.name}${it.descriptor} ${it.parameterNames}" },
            "public instance methods with code, of the class and its supertypes, without statics, bridges, the " +
                "abstract interface method, non-public methods or Object's",
        )
    }

    @Test
    fun `names come from a MethodParameters attribute when the class has one`() {
        val api = FlinkApiExtraction.extract(listOf(compile("-parameters", "-g:none")), listOf("sample.Stream"), "1.0.0")
        assertEquals(listOf("first", "second", "third", "fourth"), api.find("sample.Base", "mixed", "(JLjava/lang/String;DI)V")?.parameterNames)
    }

    @Test
    fun `a class compiled without debug information fails the extraction, naming each method without names`() {
        // javac's default, -g:source,lines, writes no LocalVariableTable.
        val failure = assertThrows<IllegalStateException> {
            FlinkApiExtraction.extract(listOf(compile("-g:source,lines")), listOf("sample.Stream"), "1.0.0")
        }
        val message = failure.message!!
        assertTrue("have no parameter names for 6 public methods" in message, message)
        assertTrue("weren't compiled with debug information (javac -g)" in message, message)
        assertTrue("  sample.Base.union([Lsample/Base;)Lsample/Base;" in message, message)
        assertTrue("  sample.Stream.accept(Ljava/lang/String;)V" in message, message)
        // A method without parameters needs no names.
        assertTrue("self" !in message, message)
    }

    @Test
    fun `classes are read from jars as from directories`() {
        val classes = compile("-g")
        val jar = File(dir, "sample.jar")
        JarOutputStream(jar.outputStream()).use { out ->
            classes.walkTopDown().filter { it.isFile }.forEach { file ->
                out.putNextEntry(JarEntry(file.relativeTo(classes).invariantSeparatorsPath))
                file.inputStream().use { it.copyTo(out) }
                out.closeEntry()
            }
        }
        assertEquals(
            FlinkApiExtraction.extract(listOf(classes), listOf("sample.Stream"), "1.0.0"),
            FlinkApiExtraction.extract(listOf(File(dir, "missing.jar"), jar), listOf("sample.Stream"), "1.0.0"),
        )
    }

    @Test
    fun `a class that isn't on the classpath fails the extraction`() {
        val failure = assertThrows<IllegalStateException> {
            FlinkApiExtraction.extract(listOf(compile("-g")), listOf("sample.Missing"), "1.0.0")
        }
        assertTrue("sample.Missing isn't on the classpath" in failure.message!!, failure.message)
    }

    @Test
    fun `the command writes the names file, and its check fails once the file differs`() {
        val classes = compile("-g")
        val file = File(dir, "flink-api.tsv")
        fun run(vararg options: String): Pair<Int, String> {
            val out = ByteArrayOutputStream()
            val status = ExtractFlinkApiCommand.run(
                listOf("--flink-version", "1.0.0", "--output", file.path, "--class", "sample.Stream", "--task", ":adapter:extractFlinkApi") +
                    options + classes.path,
                PrintStream(out, true),
                PrintStream(out, true),
            )
            return status to out.toString()
        }

        assertEquals(1, run("--check").first, "no file yet")
        assertEquals(0, run().first)
        assertEquals(FlinkApiExtraction.extract(listOf(classes), listOf("sample.Stream"), "1.0.0").write(), file.readText())
        assertEquals(0, run("--check").first)

        file.writeText(file.readText().replace("\tstreams\t", "\tothers\t"))
        val (status, output) = run("--check")
        assertEquals(1, status)
        assertTrue("Extract it again with :adapter:extractFlinkApi" in output, output)
        assertTrue("  - sample.Base\tunion\tothers\t([Lsample/Base;)Lsample/Base;" in output, output)
        assertTrue("  + sample.Base\tunion\tstreams\t([Lsample/Base;)Lsample/Base;" in output, output)
    }

    /** Compiles the sample classes with javac's [options] into a new directory. */
    private fun compile(vararg options: String): File {
        val sources = File(dir, "src/sample").apply { mkdirs() }
        SOURCES.forEach { (name, code) -> File(sources, "$name.java").writeText(code.trimIndent()) }
        val classes = File(dir, "classes${options.joinToString("") { it.replace(Regex("[^a-z]"), "") }}").apply { mkdirs() }
        val errors = ByteArrayOutputStream()
        val status = ToolProvider.getSystemJavaCompiler().run(
            null,
            null,
            errors,
            *(options.toList() + listOf("-d", classes.path) + sources.listFiles()!!.map { it.path }).toTypedArray(),
        )
        check(status == 0) { "javac failed: $errors" }
        return classes
    }

    private companion object {
        val SOURCES = mapOf(
            "Base" to """
                package sample;

                public class Base<T> {
                    @SafeVarargs
                    public final Base<T> union(Base<T>... streams) { return this; }
                    public void mixed(long first, String second, double third, int fourth) {}
                    public Base<T> self() { return this; }
                    public void accept(T value) {}
                    public static Base<String> create(String name) { return new Base<>(); }
                    protected void hidden(String name) {}
                    void local(String name) {}
                }
            """,
            "Named" to """
                package sample;

                public interface Named {
                    default String describe(String prefix) { return prefix; }
                    String label(String prefix);
                }
            """,
            // accept(String) overrides accept(T), so javac adds a bridge accept(Object).
            "Stream" to """
                package sample;

                public class Stream extends Base<String> implements Named {
                    @Override public void accept(String value) {}
                    @Override public String label(String prefix) { return prefix; }
                }
            """,
        )
    }
}
