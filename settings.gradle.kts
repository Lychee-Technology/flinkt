pluginManagement {
    includeBuild("build-logic")
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    // Provisions the pinned JDK (gradle/libs.versions.toml) when no local JDK matches it.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

rootProject.name = "flinkt"

include(":flinkt-core")
include(":flinkt-ksp")
include(":flinkt-view-codegen")
include(":test-support")

// One adapter per Flink minor line. All of them compile adapters/shared, plus their own directory.
include(":flinkt-flink23")
project(":flinkt-flink23").projectDir = file("adapters/flinkt-flink23")
include(":flinkt-flink120")
project(":flinkt-flink120").projectDir = file("adapters/flinkt-flink120")
