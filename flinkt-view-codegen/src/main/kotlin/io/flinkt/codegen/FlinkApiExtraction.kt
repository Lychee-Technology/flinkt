@file:JvmName("ExtractFlinkApi")

package io.flinkt.codegen

import java.io.Closeable
import java.io.File
import java.io.PrintStream
import java.util.zip.ZipFile
import kotlin.system.exitProcess
import org.objectweb.asm.ClassReader
import org.objectweb.asm.Opcodes
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.LabelNode
import org.objectweb.asm.tree.MethodNode

/**
 * Reads the parameter names of Flink's public instance methods from its class files, for an adapter's
 * `flink-api.tsv` (docs/architecture.md, "Parameter names").
 *
 * Flink's jars have no `MethodParameters` attribute, so the Kotlin compiler and KSP see `p0`, `p1`. Flink compiles
 * with debug information, though, and each method's `LocalVariableTable` names its parameters. A `MethodParameters`
 * attribute is preferred if a class has one.
 */
internal object FlinkApiExtraction {
    /**
     * The public instance methods of [classNames] and of their superclasses and interfaces, read from [classpath]'s jars
     * and directories, recorded as Flink [flinkVersion]'s.
     *
     * Bridge and synthetic methods are left out, and so are abstract ones: a method without code has no
     * `LocalVariableTable`, and a view's Flink class is concrete, so the generator only looks up methods with code.
     * A supertype in the JDK isn't read, because its methods aren't Flink's.
     *
     * @throws IllegalStateException if a public method with parameters has no names in its class file, which means
     *   Flink stopped compiling with debug information, or if a class isn't on [classpath].
     */
    fun extract(classpath: List<File>, classNames: List<String>, flinkVersion: String): FlinkApi = ClassPath(classpath).use { files ->
        val methods = mutableListOf<FlinkMethod>()
        val withoutNames = mutableListOf<String>()
        val requested = classNames.map { it.replace('.', '/') }.toSet()
        val visited = mutableSetOf<String>()
        val queue = ArrayDeque(requested)
        while (queue.isNotEmpty()) {
            val internalName = queue.removeFirst()
            if (!visited.add(internalName)) continue
            val bytes = files.read(internalName)
            if (bytes == null) {
                if (internalName !in requested && JDK_PACKAGES.any { internalName.startsWith(it) }) continue
                throw IllegalStateException("${internalName.replace('/', '.')} isn't on the classpath $classpath")
            }
            // Frames aren't needed. The debug information is: it holds the LocalVariableTable.
            val type = ClassNode().also { ClassReader(bytes).accept(it, ClassReader.SKIP_FRAMES) }
            type.superName?.let(queue::add)
            queue.addAll(type.interfaces)
            val className = internalName.replace('/', '.')
            for (method in type.methods) {
                if (method.access and Opcodes.ACC_PUBLIC == 0 || method.access and SKIPPED != 0 || method.name.startsWith('<')) continue
                val names = parameterNames(method)
                if (names == null) {
                    withoutNames += "$className.${method.name}${method.desc}"
                } else {
                    methods += FlinkMethod(className, method.name, method.desc, names)
                }
            }
        }
        check(withoutNames.isEmpty()) {
            "The class files have no parameter names for ${withoutNames.size} public methods: they have neither a " +
                "LocalVariableTable nor a MethodParameters attribute, so they weren't compiled with debug information " +
                "(javac -g). The forwarders can't take Flink's parameter names from them.\n" +
                withoutNames.sorted().joinToString("\n") { "  $it" }
        }
        FlinkApi(flinkVersion, methods)
    }

    /**
     * The names of [method]'s parameters: from its `MethodParameters` attribute if that names them all, else from the
     * `LocalVariableTable` entries that start at the method's first instruction, in the parameters' slots. Slot 0 is
     * `this`, and a `long` or `double` takes two slots. Null if a parameter has no name.
     */
    private fun parameterNames(method: MethodNode): List<String>? {
        val parameters = JvmDescriptors.parameters(method.desc)
        method.parameters
            ?.map { it.name }
            ?.takeIf { names -> names.size == parameters.size && names.all { !it.isNullOrEmpty() } }
            ?.let { names -> return names.map { it!! } }
        val locals = method.localVariables.orEmpty()
        // The labels before the first instruction mark offset 0, where a parameter's entry starts.
        val start = method.instructions.asSequence().takeWhile { it.opcode < 0 }.filterIsInstance<LabelNode>().toSet()
        var slot = 1
        return parameters.map { parameter ->
            val local = locals.firstOrNull { it.index == slot && it.start in start } ?: return null
            slot += JvmDescriptors.slots(parameter)
            local.name
        }
    }

    private const val SKIPPED = Opcodes.ACC_STATIC or Opcodes.ACC_ABSTRACT or Opcodes.ACC_NATIVE or Opcodes.ACC_BRIDGE or Opcodes.ACC_SYNTHETIC

    private val JDK_PACKAGES = listOf("java/", "javax/", "jdk/", "sun/")
}

/** Class files in jars and directories, found in order as a class loader finds them. */
private class ClassPath(entries: List<File>) : Closeable {
    private val jars = mutableListOf<ZipFile>()
    private val roots: List<Any> = entries.mapNotNull { entry ->
        when {
            entry.isDirectory -> entry
            entry.isFile -> ZipFile(entry).also(jars::add)
            else -> null
        }
    }

    fun read(internalName: String): ByteArray? {
        val path = "$internalName.class"
        for (root in roots) {
            when (root) {
                is File -> root.resolve(path).takeIf { it.isFile }?.let { return it.readBytes() }
                is ZipFile -> root.getEntry(path)?.let { entry -> return root.getInputStream(entry).use { it.readBytes() } }
            }
        }
        return null
    }

    override fun close() = jars.forEach { it.close() }
}

/**
 * Writes or checks an adapter's names file. The adapter's `extractFlinkApi` and `checkFlinkApi` tasks run it:
 *
 * ```text
 * --flink-version 2.3.0 --output flink-api.tsv [--check] [--task :flinkt-flink23:extractFlinkApi]
 *     --class org.apache.flink.streaming.api.datastream.DataStream ... <classpath entries>
 * ```
 *
 * With `--check`, it fails if the file isn't exactly what it would write. `--task` names the task that rewrites it.
 */
public fun main(args: Array<String>) {
    val status = ExtractFlinkApiCommand.run(args.toList(), System.out, System.err)
    if (status != 0) exitProcess(status)
}

/** [main], without leaving the JVM: [run] returns the exit status. */
internal object ExtractFlinkApiCommand {
    fun run(args: List<String>, out: PrintStream, err: PrintStream): Int = try {
        out.println(execute(args))
        0
    } catch (e: Failure) {
        err.println(e.message)
        1
    }

    private fun execute(args: List<String>): String {
        var flinkVersion: String? = null
        var output: File? = null
        var check = false
        var task = "extractFlinkApi"
        val classes = mutableListOf<String>()
        val classpath = mutableListOf<File>()
        val arguments = args.iterator()
        fun value(option: String) = if (arguments.hasNext()) arguments.next() else throw Failure("$option needs a value")
        while (arguments.hasNext()) {
            when (val argument = arguments.next()) {
                "--flink-version" -> flinkVersion = value(argument)
                "--output" -> output = File(value(argument))
                "--class" -> classes += value(argument)
                "--task" -> task = value(argument)
                "--check" -> check = true
                else -> if (argument.startsWith("--")) throw Failure("Unknown option $argument") else classpath += File(argument)
            }
        }
        val file = output ?: throw Failure("--output is missing")
        if (classes.isEmpty()) throw Failure("--class is missing")
        val api = try {
            FlinkApiExtraction.extract(classpath, classes, flinkVersion ?: throw Failure("--flink-version is missing"))
        } catch (e: IllegalStateException) {
            throw Failure(e.message ?: e.toString())
        }
        val extracted = api.write()
        if (!check) {
            file.writeText(extracted)
            return "Wrote the parameter names of ${api.methods.size} methods of Flink ${api.flinkVersion} to $file"
        }
        val current = file.takeIf { it.isFile }?.readText() ?: throw Failure("$file doesn't exist. Extract it with $task.")
        if (current != extracted) {
            val currentRows = rows(current)
            val extractedRows = rows(extracted)
            val differences = (currentRows - extractedRows).map { "  - $it" } + (extractedRows - currentRows).map { "  + $it" }
            throw Failure(
                "$file isn't what the extraction reads from the Flink class files the adapter compiles against. Extract " +
                    "it again with $task, and review its diff: it shows each parameter name that changed." +
                    differences.take(MAX_DIFFERENCES).joinToString("") { "\n$it" } +
                    (if (differences.size > MAX_DIFFERENCES) "\n  … ${differences.size - MAX_DIFFERENCES} more" else ""),
            )
        }
        return "$file has the parameter names of the ${api.methods.size} methods of Flink ${api.flinkVersion} that the extraction reads"
    }

    private fun rows(text: String): Set<String> = text.lines().filterTo(mutableSetOf()) { it.isNotEmpty() && !it.startsWith('#') }

    private class Failure(message: String) : Exception(message)

    private const val MAX_DIFFERENCES = 20
}
