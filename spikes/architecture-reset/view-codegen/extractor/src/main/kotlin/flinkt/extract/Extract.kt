// SPIKE: reads the parameter names of Flink's stream classes from their class files and writes them, keyed by
// JVM descriptor, to a file the view codegen reads. Flink's jars have no MethodParameters attribute, so the
// Kotlin compiler (and KSP) see p0, p1, ...; the names survive in the LocalVariableTable debug information.
package flinkt.extract

import org.objectweb.asm.ClassReader
import org.objectweb.asm.Opcodes
import org.objectweb.asm.Type
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.MethodNode
import java.io.File
import java.util.zip.ZipFile

private val CLASSES = listOf(
    "org.apache.flink.streaming.api.datastream.DataStream",
    "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator",
    "org.apache.flink.streaming.api.datastream.KeyedStream",
)

fun main(args: Array<String>) {
    val (version, output) = args
    val jars = args.drop(2).map(::ZipFile)
    val rows = mutableListOf<String>()
    val sources = mutableSetOf<String>()
    var failures = 0
    for (cls in CLASSES) {
        val path = cls.replace('.', '/') + ".class"
        val jar = jars.firstOrNull { it.getEntry(path) != null } ?: error("$cls is not in the given jars")
        sources += File(jar.name).name
        val node = ClassNode().also { ClassReader(jar.getInputStream(jar.getEntry(path)).readBytes()).accept(it, 0) }
        for (m in node.methods) {
            val skip = Opcodes.ACC_STATIC or Opcodes.ACC_SYNTHETIC or Opcodes.ACC_BRIDGE
            if (m.access and Opcodes.ACC_PUBLIC == 0 || m.access and skip != 0 || m.name.startsWith("<")) continue
            val names = parameterNames(m)
            if (names == null) {
                failures++
                System.err.println("no parameter names in the class file: $cls.${m.name}${m.desc}")
                continue
            }
            rows += listOf(cls, m.name, m.desc, names.joinToString(",")).joinToString("\t")
        }
    }
    File(output).writeText(
        "# Flink $version stream API parameter names, read from ${sources.sorted().joinToString()}. Regenerate; do not edit.\n" +
            "# flink-version\t$version\n" +
            rows.sorted().joinToString("\n") + "\n",
    )
    println("$output: ${rows.size} methods, $failures without names")
    if (failures > 0) kotlin.system.exitProcess(1)
}

/**
 * The MethodParameters attribute if the class has one, else the LocalVariableTable entries of the parameter
 * slots (slot 0 is `this`; long and double take two slots). A parameter's entry is the one whose range starts
 * first, since parameters are live from the method's first instruction.
 */
private fun parameterNames(m: MethodNode): List<String>? {
    val types = Type.getArgumentTypes(m.desc)
    m.parameters?.map { it.name }?.takeIf { it.size == types.size && it.none { n -> n == null } }?.let { return it }
    var slot = 1
    return types.map { t ->
        val entry = m.localVariables.orEmpty().filter { it.index == slot }.minByOrNull { m.instructions.indexOf(it.start) }
        slot += t.size
        entry?.name ?: return null
    }
}
