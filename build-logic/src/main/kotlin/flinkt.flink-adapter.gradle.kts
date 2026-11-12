// A Flink adapter: one per Flink minor line. Every adapter compiles adapters/shared, and its own src/ holds only
// what differs on its line (docs/flink-compatibility.md#differences-between-target-lines). See docs/build.md#adapters.

import io.flinkt.gradle.FlinkAdapterExtension
import io.flinkt.gradle.catalog
import io.flinkt.gradle.library

plugins {
    id("flinkt.published")
}

val adapter = extensions.create<FlinkAdapterExtension>("flinkAdapter")

val shared: Directory = layout.projectDirectory.dir("../shared")
kotlin.sourceSets {
    named("main") { kotlin.srcDir(shared.dir("src/main/kotlin")) }
    named("test") { kotlin.srcDir(shared.dir("src/test/kotlin")) }
}

val flink: Provider<String> = adapter.flinkVersion.map { "${catalog.library("flink-streaming-java").get().module}:$it" }

dependencies {
    "api"(project(":flinkt-core"))
    // The job's environment supplies Flink. The adapter compiles against its line, and its tests and compile
    // tests run on that line, as a user's job would.
    addProvider("compileOnly", flink)
    addProvider("testImplementation", flink)
    addProvider("compileTestModule", flink)
}
