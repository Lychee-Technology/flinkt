import org.gradle.api.tasks.bundling.Jar

plugins {
    id("flinkt.published")
}

dependencies {
    implementation(libs.ksp.api)

    // Snippets compiled by flinkt-ksp tests annotate records with flinkt-core's @FlinkType / @PersistedFieldOrder
    // and compile generated codecs against flinkt-core's SPI.
    "compileTestModule"(project(":flinkt-core"))
    testImplementation(project(":flinkt-core"))
    testImplementation(libs.asm.tree)
    testImplementation(gradleTestKit())
}

tasks.test {
    val processorClasspath: FileCollection = sourceSets.main.get().output
    val coreJar = project(":flinkt-core").tasks.named<Jar>("jar").flatMap { it.archiveFile }
    val kspJar = tasks.named<Jar>("jar").flatMap { it.archiveFile }
    val kotlinVersion = libs.versions.kotlin.asProvider().get()
    val kotlinMinimumVersion = libs.versions.kotlin.minimum.get()
    val kspVersion = libs.versions.ksp.get()

    inputs.files(processorClasspath).withNormalizer(ClasspathNormalizer::class).withPropertyName("kspProcessorClasspath")
    inputs.file(coreJar).withNormalizer(ClasspathNormalizer::class).withPropertyName("flinktCoreJar")
    inputs.file(kspJar).withNormalizer(ClasspathNormalizer::class).withPropertyName("flinktKspJar")

    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "-Dflinkt.test.kspProcessorClasspath=${processorClasspath.asPath}",
            "-Dflinkt.test.flinktCoreJar=${coreJar.get().asFile.path}",
            "-Dflinkt.test.flinktKspJar=${kspJar.get().asFile.path}",
            "-Dflinkt.test.kotlinVersion=$kotlinVersion",
            "-Dflinkt.test.kotlinMinimumVersion=$kotlinMinimumVersion",
            "-Dflinkt.test.kspVersion=$kspVersion",
        )
    })
}
