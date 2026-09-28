// SPIKE: extracts the parameter names of Flink's stream classes from Flink's sources jar, keyed by the JVM
// descriptor that the compiled class has, into a file the view codegen reads. Flink's jars carry no
// MethodParameters attribute, so the compiler (and KSP) see p0, p1, ...
package flinkt.extract

import com.github.javaparser.JavaParser
import com.github.javaparser.ParserConfiguration
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration
import com.github.javaparser.symbolsolver.JavaSymbolSolver
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver
import com.github.javaparser.symbolsolver.resolution.typesolvers.JarTypeSolver
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver
import java.io.File
import java.util.zip.ZipFile

private val CLASSES = listOf(
    "org.apache.flink.streaming.api.datastream.DataStream",
    "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator",
    "org.apache.flink.streaming.api.datastream.KeyedStream",
)

fun main(args: Array<String>) {
    val (version, sourcesJar, output) = args
    val solver = CombinedTypeSolver(ReflectionTypeSolver(), *args.drop(3).map { JarTypeSolver(it) }.toTypedArray())
    val parser = JavaParser(
        ParserConfiguration()
            .setLanguageLevel(ParserConfiguration.LanguageLevel.JAVA_17)
            .setSymbolResolver(JavaSymbolSolver(solver)),
    )
    val loader = java.net.URLClassLoader(args.drop(3).map { File(it).toURI().toURL() }.toTypedArray(), null)
    val rows = mutableListOf<String>()
    var failures = 0
    ZipFile(sourcesJar).use { zip ->
        for (cls in CLASSES) {
            val entry = zip.getEntry(cls.replace('.', '/') + ".java") ?: error("$cls not in $sourcesJar")
            val cu = parser.parse(zip.getInputStream(entry)).result.orElseThrow()
            val type = cu.getClassByName(cls.substringAfterLast('.')).orElseThrow()
            for (m in type.methods.filter { it.isPublic && !it.isStatic }) {
                val descriptor = runCatching { nestedFixed(varargsFixed(m.resolve().toDescriptor(), m.parameters.lastOrNull()?.isVarArgs == true), loader) }.getOrElse {
                    failures++
                    System.err.println("unresolved: $cls.${m.signature}: ${it.message}")
                    continue
                }
                val names = m.parameters.joinToString(",") { it.nameAsString }
                rows += listOf(cls, m.nameAsString, descriptor, names).joinToString("\t")
            }
        }
    }
    // Ground truth is the class file: both JavaParser and KSP's mapToJvmSignature drop the varargs array marker,
    // and JavaParser writes nested types as Outer/Inner instead of Outer$Inner.
    for (cls in CLASSES) {
        val c = Class.forName(cls, false, loader)
        val binary = c.declaredMethods
            .filter { java.lang.reflect.Modifier.isPublic(it.modifiers) && !java.lang.reflect.Modifier.isStatic(it.modifiers) && !it.isSynthetic && !it.isBridge }
            .map { "${it.name}${java.lang.invoke.MethodType.methodType(it.returnType, it.parameterTypes).toMethodDescriptorString()}" }
            .toSet()
        val extracted = rows.filter { it.startsWith("$cls\t") }.map { it.split('\t').let { c -> c[1] + c[2] } }.toSet()
        (extracted - binary).forEach { failures++; System.err.println("not in the class file: $cls.$it") }
        (binary - extracted).forEach { failures++; System.err.println("in the class file but not extracted: $cls.$it") }
    }
    File(output).writeText(
        "# Flink $version stream API, extracted from ${File(sourcesJar).name}. Regenerate; do not edit.\n" +
            "# flink-version\t$version\n" +
            rows.sorted().joinToString("\n") + "\n",
    )
    println("$output: ${rows.size} methods, $failures unresolved")
    if (failures > 0) kotlin.system.exitProcess(1)
}

/** JavaParser writes a nested type as Outer/Inner; the class file has Outer$Inner. Resolve each type against the jars. */
private fun nestedFixed(descriptor: String, loader: ClassLoader): String =
    Regex("L([^;]+);").replace(descriptor) { m ->
        var name = m.groupValues[1].replace('/', '.')
        while (runCatching { Class.forName(name, false, loader) }.isFailure && '.' in name) {
            val i = name.lastIndexOf('.')
            name = name.substring(0, i) + '$' + name.substring(i + 1)
        }
        "L" + name.replace('.', '/') + ";"
    }

/** JavaParser's descriptor omits the array marker of a varargs parameter; the class file has it. */
private fun varargsFixed(descriptor: String, lastIsVarargs: Boolean): String {
    if (!lastIsVarargs) return descriptor
    val params = descriptor.substring(1, descriptor.indexOf(')'))
    val parts = Regex("\\[*(?:L[^;]+;|[BCDFIJSZ])").findAll(params).map { it.value }.toMutableList()
    parts[parts.lastIndex] = "[" + parts.last()
    return "(" + parts.joinToString("") + descriptor.substring(descriptor.indexOf(')'))
}

