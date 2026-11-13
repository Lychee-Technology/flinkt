package io.flinkt.flink

import java.io.File
import java.lang.reflect.Method
import java.util.jar.JarFile
import org.objectweb.asm.ClassReader
import org.objectweb.asm.Opcodes
import org.objectweb.asm.Type
import org.objectweb.asm.tree.AnnotationNode
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.LdcInsnNode
import org.objectweb.asm.tree.MethodInsnNode
import org.objectweb.asm.tree.MethodNode

/**
 * What the structural tests read from class files rather than by reflection: the Flink methods that the adapter's
 * code calls, which type parameters an inline function reifies, and the stability annotations of Flink's methods,
 * which Flink 1.20 keeps in class files only (class retention).
 */
object ClassFileReader {
    private val loader: ClassLoader = ClassFileReader::class.java.classLoader

    /** The class file of [internalName], such as `org/apache/flink/streaming/api/datastream/DataStream`. */
    fun read(internalName: String): ClassNode {
        val bytes = loader.getResourceAsStream("$internalName.class")?.use { it.readBytes() }
            ?: throw AssertionError("No class file for $internalName on the test classpath")
        return ClassNode().also { ClassReader(bytes).accept(it, 0) }
    }

    fun read(type: Class<*>): ClassNode = read(Type.getInternalName(type))

    /**
     * The internal names of the classes compiled with [type] into the same output, a directory or a jar, in [type]'s
     * package: the adapter's main classes, when [type] is one of them.
     */
    fun classesBeside(type: Class<*>): List<String> {
        val location = File(type.protectionDomain.codeSource.location.toURI())
        val packagePath = type.`package`.name.replace('.', '/') + "/"
        val entries = if (location.isDirectory) {
            location.walkTopDown().filter { it.isFile }.map { it.relativeTo(location).invariantSeparatorsPath }.toList()
        } else {
            JarFile(location).use { jar -> jar.entries().asSequence().map { it.name }.toList() }
        }
        return entries
            .filter { it.startsWith(packagePath) && it.endsWith(".class") && '/' !in it.removePrefix(packagePath) }
            .map { it.removeSuffix(".class") }
            .sorted()
    }

    /** [method] as compiled, read from its declaring class's class file. */
    fun compiled(method: Method): MethodNode {
        val descriptor = Type.getMethodDescriptor(method)
        return read(method.declaringClass).methods.single { it.name == method.name && it.desc == descriptor }
    }

    /**
     * The type parameters that [method]'s body marks as reified. The Kotlin compiler marks each reified operation of
     * an inline function, such as `typeOf<R>()`, with a call to `Intrinsics.reifiedOperationMarker` that names the
     * type parameter, and its callers' compilers substitute the type argument there. Only an inline function has a
     * reified type parameter.
     */
    fun reifiedTypeParameters(method: Method): Set<String> {
        val instructions = compiled(method).instructions.toArray()
        return instructions.indices.mapNotNullTo(sortedSetOf()) { i ->
            val call = instructions[i] as? MethodInsnNode
            val name = instructions.getOrNull(i - 1) as? LdcInsnNode
            if (call?.owner == "kotlin/jvm/internal/Intrinsics" && call.name == "reifiedOperationMarker") name?.cst as? String else null
        }
    }

    /** A call instruction in a class file: the class it names, the method's name and its descriptor. */
    data class Call(val owner: String, val name: String, val descriptor: String) {
        /** Such as `DataStream.map(MapFunction, TypeInformation)`. */
        override fun toString(): String =
            owner.substringAfterLast('/') + "." + name +
                Type.getArgumentTypes(descriptor).joinToString(", ", "(", ")") { it.className.substringAfterLast('.') }
    }

    /** The calls that the methods of [classNode] make to methods of classes in [packagePrefix], such as `org/apache/flink/`. */
    fun calls(classNode: ClassNode, packagePrefix: String): Set<Call> =
        classNode.methods.flatMapTo(mutableSetOf()) { method ->
            method.instructions.toArray().filterIsInstance<MethodInsnNode>()
                .filter { it.owner.startsWith(packagePrefix) }
                .map { Call(it.owner, it.name, it.desc) }
        }

    /**
     * The method [call] reaches, as the JVM resolves it: declared by the class it names or by the nearest superclass
     * that declares it. Returns the declaring class with the method.
     */
    fun resolve(call: Call): Pair<ClassNode, MethodNode> {
        var owner: String? = call.owner
        while (owner != null) {
            val type = read(owner)
            type.methods.firstOrNull { it.name == call.name && it.desc == call.descriptor }?.let { return type to it }
            owner = type.superName
        }
        throw AssertionError("$call isn't declared by ${call.owner} or a superclass")
    }

    /** The descriptors of the annotations in a class file, whatever their retention: `RuntimeVisible` and `RuntimeInvisible`. */
    fun annotations(classNode: ClassNode): Set<String> = descriptors(classNode.visibleAnnotations, classNode.invisibleAnnotations)

    fun annotations(method: MethodNode): Set<String> = descriptors(method.visibleAnnotations, method.invisibleAnnotations)

    fun isPublic(method: MethodNode): Boolean = method.access and Opcodes.ACC_PUBLIC != 0

    private fun descriptors(visible: List<AnnotationNode>?, invisible: List<AnnotationNode>?): Set<String> =
        (visible.orEmpty() + invisible.orEmpty()).mapTo(mutableSetOf()) { it.desc }
}
