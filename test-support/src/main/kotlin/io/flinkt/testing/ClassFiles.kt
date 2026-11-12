package io.flinkt.testing

import java.io.DataInputStream

/** Facts about a compiled class that a build check asserts. */
public object ClassFiles {
    /** The class file major version: 52 for Java 8, 55 for Java 11, 61 for Java 17. */
    public fun majorVersion(type: Class<*>): Int {
        val resource = type.name.replace('.', '/') + ".class"
        val stream = type.classLoader.getResourceAsStream(resource) ?: error("no class file for ${type.name}")
        return DataInputStream(stream).use { input ->
            check(input.readInt() == 0xCAFEBABE.toInt()) { "${type.name} isn't a class file" }
            input.readUnsignedShort() // minor
            input.readUnsignedShort()
        }
    }

    /** The Kotlin metadata version the compiler wrote into [type], such as `2.4.0`. */
    public fun kotlinMetadataVersion(type: Class<*>): String =
        (type.getAnnotation(Metadata::class.java) ?: error("${type.name} has no Kotlin metadata"))
            .metadataVersion.joinToString(".")
}
