import org.gradle.api.tasks.bundling.Jar

plugins {
    id("flinkt.published")
}

val coreRuntimeScope = configurations.dependencyScope("coreRuntimeTest")
val coreRuntimeClasspath = configurations.resolvable("coreRuntimeTestClasspath") {
    extendsFrom(coreRuntimeScope.get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
}

val coreJarScope = configurations.dependencyScope("coreJarTest")
val coreJarClasspath = configurations.resolvable("coreJarTestClasspath") {
    extendsFrom(coreJarScope.get())
    isTransitive = false
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
}

dependencies {
    implementation(libs.ksp.api)

    // Snippets compiled by flinkt-ksp tests annotate records with flinkt-core's @FlinkType / @PersistedFieldOrder
    // and compile generated codecs against flinkt-core's SPI.
    "compileTestModule"(project(":flinkt-core"))
    "coreRuntimeTest"(project(":flinkt-core"))
    "coreJarTest"(project(":flinkt-core"))
    testImplementation(project(":flinkt-core"))
    testImplementation(libs.asm.tree)
    testImplementation(gradleTestKit())
}

tasks.test {
    val processorClasspath: FileCollection = sourceSets.main.get().output
    val coreRuntimeFiles: FileCollection = coreRuntimeClasspath.get()
    val coreJarFiles: FileCollection = coreJarClasspath.get()
    val kspJar = tasks.named<Jar>("jar").flatMap { it.archiveFile }
    val kotlinVersion = libs.versions.kotlin.asProvider().get()
    val kotlinMinimumVersion = libs.versions.kotlin.minimum.get()
    val kspVersion = libs.versions.ksp.get()

    inputs.files(processorClasspath).withNormalizer(ClasspathNormalizer::class).withPropertyName("kspProcessorClasspath")
    inputs.files(coreRuntimeFiles).withNormalizer(ClasspathNormalizer::class).withPropertyName("flinktCoreRuntime")
    inputs.files(coreJarFiles).withNormalizer(ClasspathNormalizer::class).withPropertyName("flinktCoreJar")
    inputs.file(kspJar).withNormalizer(ClasspathNormalizer::class).withPropertyName("flinktKspJar")

    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "-Dflinkt.test.kspProcessorClasspath=${processorClasspath.asPath}",
            "-Dflinkt.test.flinktCoreJar=${coreJarFiles.singleFile.path}",
            "-Dflinkt.test.coreRuntimeClasspath=${coreRuntimeFiles.asPath}",
            "-Dflinkt.test.flinktKspJar=${kspJar.get().asFile.path}",
            "-Dflinkt.test.kotlinVersion=$kotlinVersion",
            "-Dflinkt.test.kotlinMinimumVersion=$kotlinMinimumVersion",
            "-Dflinkt.test.kspVersion=$kspVersion",
        )
    })
}
