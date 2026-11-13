// Runs only in Flinkt's own build: the KSP processor that generates each adapter's view forwarders, and the
// extraction that reads Flink's parameter names from its class files for them. It isn't published, so it has no ABI
// dump, and checkDependencyBoundaries keeps it off every published runtime classpath.
plugins {
    id("flinkt.kotlin-jvm")
}

dependencies {
    implementation(libs.ksp.api)
    // The extraction only. The processor never loads an ASM class, so KSP needs this module's classes alone.
    implementation(libs.asm.tree)
}
