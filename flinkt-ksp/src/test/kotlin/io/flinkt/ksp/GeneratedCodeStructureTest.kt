package io.flinkt.ksp

import io.flinkt.ksp.KspTestSupport.compileSingle
import java.io.File
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.objectweb.asm.ClassReader
import org.objectweb.asm.Type
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.FieldInsnNode
import org.objectweb.asm.tree.InvokeDynamicInsnNode
import org.objectweb.asm.tree.LdcInsnNode
import org.objectweb.asm.tree.MethodInsnNode
import org.objectweb.asm.tree.TypeInsnNode

/**
 * Structural verification of generated source and bytecode:
 * - no reflection (`kotlin/reflect`, `java/lang/reflect`, `primaryConstructor`, `memberProperties`) on generated
 *   codecs or sources;
 * - no Flink references (`org/apache/flink`) anywhere in generated sources or bytecode;
 * - direct property getter calls and direct primary constructor `<init>` calls in generated codec bytecode.
 */
class GeneratedCodeStructureTest {
    @Test
    fun `generated sources and bytecode contain no reflection and no Flink references, and use direct access`() {
        val result = compileSingle(
            """
            package com.example

            import io.flinkt.FlinkType
            import io.flinkt.PersistedFieldOrder

            @FlinkType
            data class User(val id: Long, val name: String)

            @FlinkType
            data class Address(
                @property:PersistedFieldOrder(order = 10) val city: String,
                @property:PersistedFieldOrder(order = 5) var zip: Int,
            )

            @FlinkType
            data class Order(val id: Long, val user: User, val address: Address)
            """,
        ).assertSuccess()

        // 1. Inspect every generated .kt source file.
        val generatedSources = File(result.workDir, "ksp/kotlin").walkTopDown().filter { it.isFile && it.extension == "kt" }.toList()
        assertTrue(generatedSources.size >= 4, "Expected codec sources plus module source, found: $generatedSources")
        for (source in generatedSources) {
            val text = source.readText()
            for (forbidden in FORBIDDEN_SOURCE_TOKENS) {
                assertFalse(forbidden in text, "Forbidden token '$forbidden' found in ${source.name}:\n$text")
            }
        }

        // 2. Inspect every generated .class file's bytecode with ASM.
        val generatedClasses = result.classesDir.walkTopDown()
            .filter { it.isFile && it.extension == "class" }
            .filter { it.name.contains("_FlinktCodec") || it.name.startsWith("FlinktGeneratedTypeModule_") }
            .toList()
        assertTrue(generatedClasses.size >= 5, "Expected codec, factory, and module classes, found: $generatedClasses")

        for (classFile in generatedClasses) {
            val node = ClassNode().also { ClassReader(classFile.readBytes()).accept(it, 0) }
            val refs = collectReferences(node)
            val isPerRecordCodec = node.name.endsWith("_FlinktCodec") && !node.name.endsWith("\$Companion")

            for (ref in refs) {
                for (forbidden in ALWAYS_FORBIDDEN_BYTECODE_TOKENS) {
                    assertFalse(forbidden in ref, "Forbidden bytecode reference '$ref' in ${node.name}")
                }
                if ("kotlin/reflect" in ref) {
                    if (isPerRecordCodec) {
                        throw AssertionError("Per-record codec ${node.name} must not reference kotlin/reflect: '$ref'")
                    }
                    // Factories and GeneratedTypeModule call flinkt-core's SPI methods whose signatures take
                    // kotlin-stdlib's KClass and KType; no other kotlin/reflect reference is allowed.
                    assertTrue(
                        ref == "kotlin/reflect/KClass" || ref == "kotlin/reflect/KType" ||
                            ref.contains("Lkotlin/reflect/KClass;") || ref.contains("Lkotlin/reflect/KType;"),
                        "Unexpected kotlin/reflect reference '$ref' in ${node.name}",
                    )
                }
            }
        }

        // 3. Verify User_FlinktCodec bytecode uses direct getter calls and direct constructor call.
        val userCodecFile = File(result.classesDir, "com/example/User_FlinktCodec.class")
        val userNode = ClassNode().also { ClassReader(userCodecFile.readBytes()).accept(it, 0) }
        val userCalls = userNode.methods.flatMap { method ->
            method.instructions.toArray().filterIsInstance<MethodInsnNode>().map { "${it.owner}.${it.name}" }
        }.toSet()
        assertTrue("com/example/User.getId" in userCalls, "Expected direct getId call, found: $userCalls")
        assertTrue("com/example/User.getName" in userCalls, "Expected direct getName call, found: $userCalls")
        assertTrue("com/example/User.<init>" in userCalls, "Expected direct User.<init> call, found: $userCalls")
    }

    private fun collectReferences(node: ClassNode): Set<String> {
        val refs = LinkedHashSet<String>()
        node.superName?.let(refs::add)
        refs.addAll(node.interfaces.orEmpty())
        node.signature?.let(refs::add)
        for (field in node.fields.orEmpty()) {
            refs.add(field.desc)
            field.signature?.let(refs::add)
        }
        for (method in node.methods.orEmpty()) {
            refs.add(method.name)
            refs.add(method.desc)
            method.signature?.let(refs::add)
            for (insn in method.instructions.toArray()) {
                when (insn) {
                    is MethodInsnNode -> {
                        refs.add(insn.owner)
                        refs.add(insn.name)
                        refs.add(insn.desc)
                    }
                    is FieldInsnNode -> {
                        refs.add(insn.owner)
                        refs.add(insn.name)
                        refs.add(insn.desc)
                    }
                    is TypeInsnNode -> refs.add(insn.desc)
                    is InvokeDynamicInsnNode -> {
                        refs.add(insn.name)
                        refs.add(insn.desc)
                        refs.add(insn.bsm.owner)
                        refs.add(insn.bsm.name)
                    }
                    is LdcInsnNode -> when (val cst = insn.cst) {
                        is String -> refs.add(cst)
                        is Type -> refs.add(cst.descriptor)
                    }
                }
            }
        }
        return refs
    }

    private companion object {
        val FORBIDDEN_SOURCE_TOKENS = listOf(
            "kotlin.reflect",
            "java.lang.reflect",
            "primaryConstructor",
            "memberProperties",
            "org.apache.flink",
        )

        val ALWAYS_FORBIDDEN_BYTECODE_TOKENS = listOf(
            "java/lang/reflect",
            "primaryConstructor",
            "memberProperties",
            "org/apache/flink",
            "kotlin/reflect/full",
            "kotlin/reflect/jvm",
            "kotlin/reflect/KProperty",
            "kotlin/reflect/KFunction",
            "kotlin/reflect/KCallable",
        )
    }
}
