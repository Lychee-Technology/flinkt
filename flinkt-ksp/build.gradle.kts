plugins {
    id("flinkt.published")
}

dependencies {
    implementation(libs.ksp.api)
}

// A prototype of the checks KSP makes of @PersistedFieldOrder, evidence for #5's field order
// (docs/persisted-format.md#field-order), which #11 builds into this module. It has its own source set, as
// :test-support's processor does, so the processor classpath the tests hand to KSP holds the prototype and nothing
// else, and none of it is published.
val persistedOrderPrototype: SourceSet = sourceSets.create("persistedOrderPrototype")

dependencies {
    "persistedOrderPrototypeCompileOnly"(libs.ksp.api)
    // The snippets the tests compile annotate their records with flinkt-core's @FlinkType.
    "compileTestModule"(project(":flinkt-core"))
}

tasks.test {
    val processorClasspath: FileCollection = persistedOrderPrototype.output
    inputs.files(processorClasspath).withNormalizer(ClasspathNormalizer::class).withPropertyName("persistedOrderPrototypeClasspath")
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf("-Dflinkt.test.persistedOrderPrototypeClasspath=${processorClasspath.asPath}")
    })
}
