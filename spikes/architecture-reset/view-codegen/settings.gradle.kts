pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral() }
}
dependencyResolutionManagement { repositories { mavenCentral() } }
rootProject.name = "view-codegen-spike"
include(":annotations", ":processor", ":views-f23", ":views-f120")
