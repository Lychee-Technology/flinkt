plugins {
    id("flinkt.flink-adapter")
}

flinkAdapter {
    flinkVersion = libs.versions.flink23
}

// Flink 2.2 has no adapter until #27. Until it has one, the record format's evidence on 2.2.1 runs here
// (docs/persisted-format.md#evidence): the shared record and string format tests and their prototype, compiled and
// run against Flink 2.2.1, so the string format's byte equality with StringValue and the bytes on both output views
// are tested on every target line. `check` runs it, so the primary PR lane does. #27's adapter replaces it.
val flink22FormatSources = listOf(
    "RecordFormat.kt",
    "OutputViews.kt",
    "Examples.kt",
    "RecordFormatTest.kt",
    "StringFormatTest.kt",
    "FormatFlinkLineTest.kt",
)
val flink22Format: SourceSet = sourceSets.create("flink22Format")
kotlin.sourceSets.named(flink22Format.name) {
    kotlin.srcDir(layout.projectDirectory.dir("../shared/src/test/kotlin"))
    kotlin.setIncludes(flink22FormatSources.map { "io/flinkt/flink/format/$it" })
}
dependencies {
    "flink22FormatImplementation"(project(":flinkt-core"))
    "flink22FormatImplementation"(libs.flink.streaming.java) { version { strictly(libs.versions.flink22.get()) } }
    "flink22FormatImplementation"(platform(libs.junit.bom))
    "flink22FormatImplementation"(libs.junit.jupiter)
    "flink22FormatRuntimeOnly"(libs.junit.platform.launcher)
}
val flink22FormatTest = tasks.register<Test>("flink22FormatTest") {
    description = "Runs the record format's evidence on Flink ${libs.versions.flink22.get()}, which has no adapter yet."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    testClassesDirs = flink22Format.output.classesDirs
    classpath = flink22Format.runtimeClasspath
    // FormatFlinkLineTest fails unless the tests run on this release.
    systemProperty("flinkt.test.formatFlinkVersion", libs.versions.flink22.get())
}
tasks.named("check") {
    dependsOn(flink22FormatTest)
}
