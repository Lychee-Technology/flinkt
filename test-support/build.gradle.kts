// Test code shared by every module's tests: the exact-type carrier and the compile-test harness.
// It isn't published. A published flinkt-testkit waits until something outside the repository needs it
// (docs/testing.md#testkit).
plugins {
    id("flinkt.compile-tests")
}

// A KSP processor that only the harness's own tests run. It has its own source set, so the processor classpath
// they hand to KSP holds the processor and nothing else.
val testProcessor: SourceSet = sourceSets.create("testProcessor")

dependencies {
    "testProcessorCompileOnly"(libs.ksp.api)
}

tasks.test {
    val processorClasspath: FileCollection = testProcessor.output
    inputs.files(processorClasspath).withNormalizer(ClasspathNormalizer::class).withPropertyName("testProcessorClasspath")
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf("-Dflinkt.test.processorClasspath=${processorClasspath.asPath}")
    })
}
