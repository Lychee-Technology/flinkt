plugins {
    id("flinkt.published")
}

tasks.checkDependencyBoundaries {
    val stdlibOnly = "flinkt-core depends on the Kotlin standard library only. Model modules and generated code " +
        "depend on it, and it doesn't change when Flink does (docs/architecture.md#layers-and-modules)."
    val stdlib = listOf("org.jetbrains.kotlin:kotlin-stdlib", "org.jetbrains:annotations")
    allowOnly("compileClasspath", stdlib, reason = stdlibOnly)
    allowOnly("runtimeClasspath", stdlib, reason = stdlibOnly)
    forbid("testCompileClasspath", groups = listOf("org.apache.flink"), reason = stdlibOnly)
    forbid("testRuntimeClasspath", groups = listOf("org.apache.flink"), reason = stdlibOnly)
}
