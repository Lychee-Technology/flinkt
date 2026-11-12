package io.flinkt.gradle

import java.io.File
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * `checkDependencyBoundaries` on Gradle's own dependency resolution. Each test declares the dependencies it checks in
 * a fresh build, registers the task on the classpaths that `flinkt.published` checks, and runs the task's action.
 * Modules come from a Maven repository in the test's directory, so nothing is downloaded.
 */
class DependencyBoundaryCheckTest {
    @TempDir
    lateinit var dir: File

    private val repository: File get() = dir.resolve("repository")

    private val root: Project by lazy {
        ProjectBuilder.builder()
            .withName("root")
            .withProjectDir(dir.resolve("root").apply { mkdirs() })
            .withGradleUserHomeDir(dir.resolve("gradle-user-home"))
            .build()
    }

    @Test
    fun `a classpath of allowed modules passes`() {
        module("org.jetbrains.kotlin:kotlin-stdlib:2.4.0")
        val app = javaLibrary("app") { dependencies.add("implementation", "org.jetbrains.kotlin:kotlin-stdlib:2.4.0") }

        app.boundaryCheck {
            allowOnly("compileClasspath", STDLIB, reason = "stdlib only")
            allowOnly("runtimeClasspath", STDLIB, reason = "stdlib only")
        }.verify()
    }

    @Test
    fun `a module in a forbidden group fails, with the path that brought it in`() {
        module("org.apache.flink:flink-core:1.20.5")
        module("org.example:wrapper:1.0", "org.apache.flink:flink-core:1.20.5")
        val app = javaLibrary("app") { dependencies.add("implementation", "org.example:wrapper:1.0") }

        val check = app.boundaryCheck { forbid("runtimeClasspath", groups = listOf("org.apache.flink"), reason = "no Flink") }

        assertEquals(
            """
            Dependency boundary violated in :app:
              - runtimeClasspath: no Flink
                  org.apache.flink:flink-core:1.20.5, through project ':app' -> org.example:wrapper:1.0 -> org.apache.flink:flink-core:1.20.5
            """.trimIndent(),
            failure(check),
        )
    }

    @Test
    fun `a forbidden project fails`() {
        javaLibrary("flinkt-ksp")
        val app = javaLibrary("app") { dependencies.add("implementation", project(":flinkt-ksp")) }

        val check = app.boundaryCheck { forbid("runtimeClasspath", projects = listOf(":flinkt-ksp"), reason = "build time only") }

        assertEquals(
            """
            Dependency boundary violated in :app:
              - runtimeClasspath: build time only
                  project :flinkt-ksp, through project ':app' -> project ':flinkt-ksp'
            """.trimIndent(),
            failure(check),
        )
    }

    // Gradle leaves a file dependency out of the resolved graph, so a check that only walks the graph passes a Flink
    // jar added with files().
    @Test
    fun `a file dependency fails an allowlist`() {
        module("org.jetbrains.kotlin:kotlin-stdlib:2.4.0")
        val flink = jar("libs/flink-core-1.20.5.jar")
        val app = javaLibrary("app") {
            dependencies.add("implementation", "org.jetbrains.kotlin:kotlin-stdlib:2.4.0")
            dependencies.add("implementation", files(flink))
        }

        val check = app.boundaryCheck {
            allowOnly("compileClasspath", STDLIB, reason = "stdlib only")
            allowOnly("runtimeClasspath", STDLIB, reason = "stdlib only")
        }

        assertEquals(
            """
            Dependency boundary violated in :app:
              - compileClasspath: stdlib only
                  file dependency <dir>/libs/flink-core-1.20.5.jar, which isn't one of [org.jetbrains.kotlin:kotlin-stdlib], declared with files() in this module or in a project it depends on
              - runtimeClasspath: stdlib only
                  file dependency <dir>/libs/flink-core-1.20.5.jar, which isn't one of [org.jetbrains.kotlin:kotlin-stdlib], declared with files() in this module or in a project it depends on
            """.trimIndent(),
            failure(check),
        )
    }

    @Test
    fun `a file dependency fails a forbid rule`() {
        val flink = jar("libs/flink-core-1.20.5.jar")
        val app = javaLibrary("app") { dependencies.add("testImplementation", files(flink)) }

        val check = app.boundaryCheck {
            forbid("runtimeClasspath", groups = listOf("org.apache.flink"), reason = "no Flink")
            forbid("testRuntimeClasspath", groups = listOf("org.apache.flink"), reason = "no Flink in tests")
        }

        assertEquals(
            """
            Dependency boundary violated in :app:
              - testRuntimeClasspath: no Flink in tests
                  file dependency <dir>/libs/flink-core-1.20.5.jar, which has no coordinates to check, declared with files() in this module or in a project it depends on
            """.trimIndent(),
            failure(check),
        )
    }

    @Test
    fun `a file dependency that a project dependency brings in fails`() {
        val flink = jar("libs/flink-core-1.20.5.jar")
        javaLibrary("lib") { dependencies.add("api", files(flink)) }
        val app = javaLibrary("app") { dependencies.add("implementation", project(":lib")) }

        val check = app.boundaryCheck { forbid("runtimeClasspath", groups = listOf("org.apache.flink"), reason = "no Flink") }

        assertEquals(
            """
            Dependency boundary violated in :app:
              - runtimeClasspath: no Flink
                  file dependency <dir>/libs/flink-core-1.20.5.jar, which has no coordinates to check, declared with files() in this module or in a project it depends on
            """.trimIndent(),
            failure(check),
        )
    }

    /** A `java-library` project under [root], resolving modules from [repository]. */
    private fun javaLibrary(name: String, configure: Project.() -> Unit = {}): Project {
        val project = ProjectBuilder.builder()
            .withParent(root)
            .withName(name)
            .withProjectDir(dir.resolve(name).apply { mkdirs() })
            .build()
        project.pluginManager.apply("java-library")
        project.repositories.maven { url = project.uri(repository) }
        project.configure()
        return project
    }

    /** Registers the check on the classpaths that `flinkt.published` checks. */
    private fun Project.boundaryCheck(rules: DependencyBoundaryCheck.() -> Unit): DependencyBoundaryCheck {
        val projectPath = path
        val check = tasks.register("checkDependencyBoundaries", DependencyBoundaryCheck::class.java).get()
        check.modulePath.set(projectPath)
        for (name in listOf("compileClasspath", "runtimeClasspath", "testCompileClasspath", "testRuntimeClasspath")) {
            check.classpath(configurations.named(name))
        }
        check.rules()
        return check
    }

    /** The check's failure, with the test's directory written as `<dir>`. */
    private fun failure(check: DependencyBoundaryCheck): String =
        assertThrows<GradleException> { check.verify() }.message.orEmpty()
            .replace(dir.canonicalPath, "<dir>")
            .replace(dir.absolutePath, "<dir>")

    /** Publishes `group:name:version` to [repository], with compile-scope [dependencies]. */
    private fun module(coordinates: String, vararg dependencies: String) {
        val (group, name, version) = coordinates.split(':')
        val directory = repository.resolve("${group.replace('.', '/')}/$name/$version").apply { mkdirs() }
        val dependencyElements = dependencies.joinToString("") {
            val (dependencyGroup, dependencyName, dependencyVersion) = it.split(':')
            "<dependency><groupId>$dependencyGroup</groupId><artifactId>$dependencyName</artifactId>" +
                "<version>$dependencyVersion</version></dependency>"
        }
        directory.resolve("$name-$version.pom").writeText(
            """<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>""" +
                "<groupId>$group</groupId><artifactId>$name</artifactId><version>$version</version>" +
                "<dependencies>$dependencyElements</dependencies></project>",
        )
        jar(directory.resolve("$name-$version.jar").relativeTo(dir).path)
    }

    /** An empty jar at [path] in the test's directory. */
    private fun jar(path: String): File =
        dir.resolve(path).apply {
            parentFile.mkdirs()
            ZipOutputStream(outputStream()).use { zip ->
                zip.putNextEntry(ZipEntry("META-INF/MANIFEST.MF"))
                zip.write("Manifest-Version: 1.0\n".toByteArray())
                zip.closeEntry()
            }
        }

    private companion object {
        val STDLIB = listOf("org.jetbrains.kotlin:kotlin-stdlib")
    }
}
