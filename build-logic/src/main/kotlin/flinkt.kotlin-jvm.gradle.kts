// Every Kotlin module in the build. The reasons for each choice are in docs/build.md#toolchain.

import io.flinkt.gradle.catalog
import io.flinkt.gradle.library
import io.flinkt.gradle.version
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id("org.jetbrains.kotlin.jvm")
}

kotlin {
    jvmToolchain(catalog.version("jdk").toInt())

    // The default kotlin-stdlib dependency, so also the one published modules declare, is the minimum supported
    // compiler's. API version 2.4 keeps newer standard-library API out of Flinkt's classes.
    coreLibrariesVersion = catalog.version("kotlin-minimum")

    compilerOptions {
        // Language version 2.4 makes the build compiler write Kotlin metadata 2.4.0, whatever its own version, so
        // a 2.4.0 compiler reads every class the build produces, :test-support's included.
        languageVersion = KotlinVersion.KOTLIN_2_4
        apiVersion = KotlinVersion.KOTLIN_2_4
    }
}

dependencies {
    "testImplementation"(platform(catalog.library("junit-bom")))
    "testImplementation"(catalog.library("junit-jupiter"))
    "testRuntimeOnly"(catalog.library("junit-platform-launcher"))
}

// A comma-separated list of JUnit tags to skip, such as `minicluster` on a lane that runs no MiniCluster.
val excludedTestTags: List<String> =
    providers.gradleProperty("flinkt.test.excludeTags").orNull
        ?.split(',')?.map { it.trim() }?.filter { it.isNotEmpty() }
        .orEmpty()

tasks.withType<Test>().configureEach {
    useJUnitPlatform {
        if (excludedTestTags.isNotEmpty()) excludeTags(*excludedTestTags.toTypedArray())
    }
    testLogging {
        events("failed")
        exceptionFormat = TestExceptionFormat.FULL
    }
}
