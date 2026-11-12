// A module that Flinkt publishes: explicit API, a checked-in ABI dump, and the dependency boundaries.
// See docs/build.md#public-api and docs/build.md#dependency-boundaries.

import io.flinkt.gradle.DependencyBoundaryCheck
import io.flinkt.gradle.catalog
import io.flinkt.gradle.version
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation
import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile

plugins {
    id("flinkt.kotlin-jvm")
    id("flinkt.compile-tests")
}

// The published classes run on every JDK that a target Flink line supports, Java 8 for Flink 1.20 included.
// -Xjdk-release also holds them to the Java 8 platform API, which the toolchain JDK alone wouldn't.
// Tests compile for the toolchain JDK they run on.
val publishedJvmTarget: String = catalog.version("jvm-target")
tasks.named<KotlinJvmCompile>("compileKotlin") {
    compilerOptions {
        jvmTarget = JvmTarget.fromTarget(publishedJvmTarget)
        freeCompilerArgs.add("-Xjdk-release=$publishedJvmTarget")
    }
}
tasks.named<JavaCompile>("compileJava") {
    options.release = JavaVersion.toVersion(publishedJvmTarget).majorVersion.toInt()
}

kotlin {
    explicitApi()

    // A checked-in dump per module, in api/. `check` fails when the public ABI differs from it.
    @OptIn(ExperimentalAbiValidation::class)
    abiValidation()
}

val projectPath: String = path
val checkDependencyBoundaries = tasks.register<DependencyBoundaryCheck>("checkDependencyBoundaries") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Fails if a classpath of this module crosses an architecture dependency boundary."
    modulePath = projectPath
    for (name in listOf("compileClasspath", "runtimeClasspath", "testCompileClasspath", "testRuntimeClasspath")) {
        classpath(configurations.named(name))
    }
    forbid(
        "runtimeClasspath",
        projects = listOf(":flinkt-ksp", ":flinkt-view-codegen"),
        reason = "Build-time modules never reach a runtime classpath: flinkt-ksp runs on KSP's processor " +
            "classpath, and flinkt-view-codegen only in Flinkt's own build (docs/architecture.md#layers-and-modules).",
    )
    forbid(
        "runtimeClasspath",
        groups = listOf("org.apache.flink"),
        reason = "Flinkt never packages a Flink runtime. The job's environment supplies Flink, so an adapter " +
            "takes it as compileOnly (docs/flink-compatibility.md#dependency-packaging).",
    )
}

@OptIn(ExperimentalAbiValidation::class)
tasks.named("check") {
    dependsOn(checkDependencyBoundaries, kotlin.abiValidation.checkTaskProvider)
}
