// Wires the compile-test harness (:test-support) into this module's tests: the two pinned Kotlin compilers,
// the pinned KSP, and the module's classpath as a consumer sees it. See docs/build.md#compile-test-harness.

import io.flinkt.gradle.CompileTestProperties
import io.flinkt.gradle.catalog
import io.flinkt.gradle.library
import io.flinkt.gradle.version

plugins {
    id("flinkt.kotlin-jvm")
}

fun module(alias: String, version: String): String = "${catalog.library(alias).get().module}:$version"

/** A dependency scope and the runtime classpath resolved from it. */
fun runtimeClasspath(name: String, vararg notations: Any): NamedDomainObjectProvider<ResolvableConfiguration> {
    val scope = configurations.dependencyScope(name)
    notations.forEach { dependencies.add(name, it) }
    return configurations.resolvable("${name}Classpath") {
        extendsFrom(scope.get())
        attributes { attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME)) }
    }
}

val buildCompiler = runtimeClasspath(
    "compileTestKotlinBuild",
    module("kotlin-compiler-embeddable", catalog.version("kotlin")),
)
val minimumCompiler = runtimeClasspath(
    "compileTestKotlinMinimum",
    module("kotlin-compiler-embeddable", catalog.version("kotlin-minimum")),
)
val ksp = runtimeClasspath(
    "compileTestKsp",
    catalog.library("ksp-standalone").get(),
    catalog.library("ksp-api").get(),
    catalog.library("ksp-common-deps").get(),
)

// The module as a consumer compiles against it: its API elements and their transitive API. Adapter modules add
// their Flink line here, as a user's job would. :test-support supplies `exactTypeOf`.
val snippetScope = configurations.dependencyScope("compileTestModule")
val snippetClasspath = configurations.resolvable("compileTestModuleClasspath") {
    extendsFrom(snippetScope.get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_API))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
}
dependencies {
    "compileTestModule"(project(path))
    if (path != ":test-support") {
        "compileTestModule"(project(":test-support"))
        "testImplementation"(project(":test-support"))
    }
}

val compileTestProperties = objects.newInstance<CompileTestProperties>().apply {
    buildCompilerVersion = catalog.version("kotlin")
    buildCompilerClasspath.from(buildCompiler)
    minimumCompilerVersion = catalog.version("kotlin-minimum")
    minimumCompilerClasspath.from(minimumCompiler)
    kspVersion = catalog.version("ksp")
    kspClasspath.from(ksp)
    moduleClasspath.from(snippetClasspath)
    workDir = layout.buildDirectory.dir("compile-tests")
}

tasks.named<Test>("test") {
    jvmArgumentProviders.add(compileTestProperties)
}
