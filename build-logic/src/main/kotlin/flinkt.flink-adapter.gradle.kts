// A Flink adapter: one per Flink minor line. Every adapter compiles adapters/shared, and its own src/ holds only
// what differs on its line (docs/flink-compatibility.md#differences-between-target-lines). See docs/build.md#adapters.

import io.flinkt.gradle.FlinkAdapterExtension
import io.flinkt.gradle.FlinkApiExtractionArguments
import io.flinkt.gradle.ViewCodegenArguments
import io.flinkt.gradle.ViewCodegenTestProperties
import io.flinkt.gradle.catalog
import io.flinkt.gradle.library
import io.flinkt.gradle.selectedVersion
import io.flinkt.gradle.version

plugins {
    id("flinkt.published")
    id("com.google.devtools.ksp")
}

val adapter = extensions.create<FlinkAdapterExtension>("flinkAdapter")

val shared: Directory = layout.projectDirectory.dir("../shared")
kotlin.sourceSets {
    named("main") { kotlin.srcDir(shared.dir("src/main/kotlin")) }
    named("test") { kotlin.srcDir(shared.dir("src/test/kotlin")) }
}

val flinkModule = catalog.library("flink-streaming-java").get().module
val flink: Provider<String> = adapter.flinkVersion.map { "$flinkModule:$it" }

dependencies {
    "api"(project(":flinkt-core"))
    // The job's environment supplies Flink. The adapter compiles against its line, and its tests and compile
    // tests run on that line, as a user's job would.
    addProvider("compileOnly", flink)
    addProvider("testImplementation", flink)
    addProvider("compileTestModule", flink)

    // flinkt-view-codegen generates the views' forwarders. @FlinkView has source retention, so the module is on the
    // compile classpath only, and checkDependencyBoundaries keeps it off the runtime classpath. It's built for the
    // toolchain JDK that runs it. No class of it reaches the adapter's Java 8 bytecode, so the compile classpath asks
    // for it at the toolchain's level. Compile tests declare views of their own.
    "ksp"(project(":flinkt-view-codegen"))
    "compileOnly"(project(":flinkt-view-codegen")) {
        attributes { attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, catalog.version("jdk").toInt()) }
    }
    "compileTestModule"(project(":flinkt-view-codegen"))

    // The views' structural tests read class files: the views', for what hand-written code calls and reifies, and
    // Flink's, for the stability annotations that 1.20 keeps only there (class retention).
    "testImplementation"(catalog.library("asm-tree"))
}

// The view generator's inputs (docs/architecture.md#parameter-names): the names file checked in next to this build
// script, and the Flink version on the compile classpath, which the file has to have been extracted from.
val namesFile: RegularFile = layout.projectDirectory.file("flink-api.tsv")
val compileClasspath = configurations.named("compileClasspath")
val flinkVersionOnClasspath: Provider<String> = compileClasspath
    .flatMap { it.incoming.resolutionResult.rootComponent }
    .map { it.selectedVersion(flinkModule.group, flinkModule.name) }

ksp {
    arg(
        objects.newInstance<ViewCodegenArguments>().apply {
            flinkApiFile = namesFile
            flinkVersion = flinkVersionOnClasspath
        },
    )
}

// The extraction reads the Flink classes the views hold, and their supertypes, from the Flink jars the adapter
// compiles against. `extractFlinkApi` writes the names file, and `checkFlinkApi`, part of `check`, fails if the
// checked-in file isn't what it would write.
val viewedFlinkClasses = listOf(
    "org.apache.flink.streaming.api.datastream.DataStream",
    "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator",
    "org.apache.flink.streaming.api.datastream.KeyedStream",
)
val flinkJars: Provider<FileCollection> = compileClasspath.map { classpath ->
    classpath.incoming.artifactView {
        componentFilter { it is ModuleComponentIdentifier && it.group == flinkModule.group }
    }.files
}
val extraction = configurations.dependencyScope("flinkApiExtraction")
val extractionClasspath = configurations.resolvable("flinkApiExtractionClasspath") {
    extendsFrom(extraction.get())
    attributes { attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME)) }
}
dependencies {
    "flinkApiExtraction"(project(":flinkt-view-codegen"))
}

val extractTaskPath = "$path:extractFlinkApi"
fun extractionArguments(check: Boolean) = objects.newInstance<FlinkApiExtractionArguments>().apply {
    flinkClasspath.from(flinkJars)
    flinkVersion = flinkVersionOnClasspath
    classes = viewedFlinkClasses
    flinkApiFile = namesFile
    this.check = check
    extractTask = extractTaskPath
}

tasks.register<JavaExec>("extractFlinkApi") {
    description = "Extracts the parameter names of the viewed Flink classes' methods into flink-api.tsv."
    group = LifecycleBasePlugin.BUILD_GROUP
    classpath(extractionClasspath)
    mainClass = "io.flinkt.codegen.ExtractFlinkApi"
    val arguments = extractionArguments(check = false)
    argumentProviders.add(arguments)
    outputs.file(arguments.flinkApiFile)
}

val checkFlinkApi = tasks.register<JavaExec>("checkFlinkApi") {
    description = "Fails if flink-api.tsv isn't what extractFlinkApi would write from the Flink jars on the compile classpath."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    classpath(extractionClasspath)
    mainClass = "io.flinkt.codegen.ExtractFlinkApi"
    val arguments = extractionArguments(check = true)
    argumentProviders.add(arguments)
    inputs.files(arguments.flinkApiFile).withPathSensitivity(PathSensitivity.NONE).withPropertyName("flinkApiFile")
    // An output, so that the check is up to date until an input changes.
    val marker = layout.buildDirectory.file("flink-api-check/checked")
    outputs.file(marker)
    doLast { marker.get().asFile.writeText("checked\n") }
}
tasks.named("check") {
    dependsOn(checkFlinkApi)
}

// The generator's tests run flinkt-view-codegen in the compile-test harness, with this adapter's names file.
val viewCodegen = configurations.dependencyScope("viewCodegenProcessor")
val viewCodegenClasspath = configurations.resolvable("viewCodegenProcessorClasspath") {
    extendsFrom(viewCodegen.get())
    isTransitive = false
    attributes { attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME)) }
}
dependencies {
    "viewCodegenProcessor"(project(":flinkt-view-codegen"))
}

// The cross-adapter comparison (docs/testing.md#generated-forwarders) reads the forwarders that every adapter
// generated. Each adapter offers the Kotlin that KSP generated for it as a variant of its own usage, which no Java
// classpath asks for, and its tests resolve that variant of every adapter: each project in adapters/, this one
// included. So each lane's comparison runs the generation of the other adapters too, but none of their tests.
val forwardersUsage: Usage = objects.named("flinkt-view-forwarders")
val forwardersElements = configurations.consumable("viewForwardersElements") {
    attributes { attribute(Usage.USAGE_ATTRIBUTE, forwardersUsage) }
}
artifacts {
    add(forwardersElements.name, layout.buildDirectory.dir("generated/ksp/main/kotlin")) {
        type = ArtifactTypeDefinition.DIRECTORY_TYPE
        builtBy("kspKotlin")
    }
}
val adapterForwarders = configurations.dependencyScope("adapterForwarders")
val adapterForwardersFiles = configurations.resolvable("adapterForwardersFiles") {
    extendsFrom(adapterForwarders.get())
    attributes { attribute(Usage.USAGE_ATTRIBUTE, forwardersUsage) }
}
val adaptersDirectory: File = rootProject.file("adapters")
dependencies {
    for (adapterProject in rootProject.subprojects.filter { it.projectDir.parentFile == adaptersDirectory }) {
        "adapterForwarders"(project(adapterProject.path))
    }
}

tasks.named<Test>("test") {
    jvmArgumentProviders.add(
        objects.newInstance<ViewCodegenTestProperties>().apply {
            processorClasspath.from(viewCodegenClasspath)
            flinkApiFile = namesFile
            flinkVersion = flinkVersionOnClasspath
            adapterForwarders.from(adapterForwardersFiles)
        },
    )
}
