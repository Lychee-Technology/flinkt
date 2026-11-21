package io.flinkt.ksp

import io.flinkt.ksp.KspTestSupport.codecFor
import io.flinkt.ksp.KspTestSupport.roundTrip
import io.flinkt.registry.TypeRegistry
import java.io.File
import java.net.URLClassLoader
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Incremental smoke test on a real Gradle + KSP build (docs/testing.md, "Incremental compilation").
 *
 * Exercises the incremental scenarios required by #11 (unreferenced edit, referenced field change, removing
 * `@FlinkType` from a referenced type, plus adding and deleting declarations), comparing every incremental build's
 * generated sources, service file, and diagnostics with a fresh clean build of the same sources.
 */
class KspIncrementalGradleTest {
    @TempDir
    lateinit var workspace: File

    private val coreJar = File(System.getProperty("flinkt.test.flinktCoreJar"))
    private val kspJar = File(System.getProperty("flinkt.test.flinktKspJar"))
    private val kotlinVersion = System.getProperty("flinkt.test.kotlinVersion")
    private val kotlinMinimumVersion = System.getProperty("flinkt.test.kotlinMinimumVersion")
    private val kspVersion = System.getProperty("flinkt.test.kspVersion")

    @Test
    fun `incremental KSP builds match clean builds across edits, additions, deletions, and invalidation`() {
        val projectDir = workspace.resolve("sample").apply { mkdirs() }
        writeProjectBuildFiles(projectDir)

        val srcDir = projectDir.resolve("src/main/kotlin/com/example").apply { mkdirs() }
        val addressFile = srcDir.resolve("Address.kt")
        val userFile = srcDir.resolve("User.kt")
        val orderFile = srcDir.resolve("Order.kt")

        addressFile.writeText(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class Address(val city: String)
            """.trimIndent() + "\n",
        )
        userFile.writeText(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class User(val id: Long, val address: Address)
            """.trimIndent() + "\n",
        )
        orderFile.writeText(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class Order(val id: Long, val amount: Long)
            """.trimIndent() + "\n",
        )

        // 1. Clean baseline build.
        runGradle(projectDir, "clean", "classes")
        val kspOutputDir = projectDir.resolve("build/generated/ksp/main")
        val userCodecFile = kspOutputDir.resolve("kotlin/com/example/User_FlinktCodec.kt")
        val addressCodecFile = kspOutputDir.resolve("kotlin/com/example/Address_FlinktCodec.kt")
        val initialUserCodecSource = userCodecFile.readText()
        val initialAddressCodecSource = addressCodecFile.readText()

        // 2. Edit an unreferenced declaration (Order.kt): incremental build updates Order and registry, keeps User and
        // Address codec sources byte-for-byte identical (so Gradle's content-hashed compileKotlin sees only Order
        // changed), and matches a clean build.
        orderFile.writeText(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class Order(val id: Long, val amount: Long, val note: String)
            """.trimIndent() + "\n",
        )
        val orderEditBuild = runGradle(projectDir, "classes", "--info")
        val afterOrderEditSnapshot = snapshotKspOutputs(kspOutputDir)
        assertEquals(initialUserCodecSource, userCodecFile.readText())
        assertEquals(initialAddressCodecSource, addressCodecFile.readText())
        assertTrue("note" in kspOutputDir.resolve("kotlin/com/example/Order_FlinktCodec.kt").readText())
        assertFalse(
            "User_FlinktCodec.kt has changed" in orderEditBuild.output,
            "compileKotlin should not see unchanged User_FlinktCodec.kt as changed:\n${orderEditBuild.output}",
        )
        assertFalse(
            "Address_FlinktCodec.kt has changed" in orderEditBuild.output,
            "compileKotlin should not see unchanged Address_FlinktCodec.kt as changed:\n${orderEditBuild.output}",
        )
        assertMatchesCleanBuild(projectDir, afterOrderEditSnapshot)

        withCompiledClassLoader(projectDir) { loader ->
            val registry = TypeRegistry.load(loader)
            assertEquals(
                setOf("com.example.Address", "com.example.Order", "com.example.User"),
                registry.declarations.map { it.schema.typeName }.toSet(),
            )
            val orderClass = loader.loadClass("com.example.Order")
            val order = orderClass.getConstructor(Long::class.javaPrimitiveType, Long::class.javaPrimitiveType, String::class.java)
                .newInstance(1L, 50L, "rush")
            assertEquals(order, roundTrip(codecFor(registry, loader, "com.example.Order"), order))
        }

        // 3. Change Address's fields: User's generated source stays identical, and User round-trips with the new Address.
        addressFile.writeText(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class Address(val city: String, val zip: String)
            """.trimIndent() + "\n",
        )
        runGradle(projectDir, "classes")
        val afterAddressEditSnapshot = snapshotKspOutputs(kspOutputDir)
        assertEquals(
            initialUserCodecSource,
            userCodecFile.readText(),
            "Changing Address's fields must leave User's generated source unchanged",
        )
        assertMatchesCleanBuild(projectDir, afterAddressEditSnapshot)

        withCompiledClassLoader(projectDir) { loader ->
            val registry = TypeRegistry.load(loader)
            val addressClass = loader.loadClass("com.example.Address")
            val userClass = loader.loadClass("com.example.User")
            val address = addressClass.getConstructor(String::class.java, String::class.java).newInstance("Berlin", "10115")
            val user = userClass.getConstructor(Long::class.javaPrimitiveType, addressClass).newInstance(42L, address)
            assertEquals(user, roundTrip(codecFor(registry, loader, "com.example.User"), user))
        }

        // 4. Remove @FlinkType from Address: incremental build fails with the User.address diagnostic, as a clean build does.
        addressFile.writeText(
            """
            package com.example

            data class Address(val city: String, val zip: String)
            """.trimIndent() + "\n",
        )
        val incrementalFailure = runGradleExpectFailure(projectDir, "classes")
        val expectedDiagnostic = "com.example.User.address: com.example.Address isn't a built-in type or a @FlinkType data class"
        assertTrue(
            expectedDiagnostic in incrementalFailure.output,
            "Expected User.address diagnostic on incremental build, got:\n${incrementalFailure.output}",
        )

        val cleanFailure = runGradleExpectFailure(projectDir, "clean", "classes")
        assertTrue(
            expectedDiagnostic in cleanFailure.output,
            "Expected User.address diagnostic on clean build, got:\n${cleanFailure.output}",
        )

        // 5. Restore Address, delete Order.kt, and add Product.kt: incremental build cleans up stale outputs and
        // updates the aggregating module and service file identically to a clean build.
        addressFile.writeText(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class Address(val city: String, val zip: String)
            """.trimIndent() + "\n",
        )
        runGradle(projectDir, "classes")

        assertTrue(orderFile.delete())
        val productFile = srcDir.resolve("Product.kt")
        productFile.writeText(
            """
            package com.example

            import io.flinkt.FlinkType

            @FlinkType
            data class Product(val sku: String, val priceCents: Long)
            """.trimIndent() + "\n",
        )
        runGradle(projectDir, "classes")
        val afterDeleteAndAddSnapshot = snapshotKspOutputs(kspOutputDir)
        assertFalse(kspOutputDir.resolve("kotlin/com/example/Order_FlinktCodec.kt").exists())
        assertTrue(kspOutputDir.resolve("kotlin/com/example/Product_FlinktCodec.kt").exists())
        assertMatchesCleanBuild(projectDir, afterDeleteAndAddSnapshot)

        withCompiledClassLoader(projectDir) { loader ->
            val registry = TypeRegistry.load(loader)
            assertEquals(
                setOf("com.example.Address", "com.example.Product", "com.example.User"),
                registry.declarations.map { it.schema.typeName }.toSet(),
            )
            val productClass = loader.loadClass("com.example.Product")
            val product = productClass.getConstructor(String::class.java, Long::class.javaPrimitiveType).newInstance("SKU-1", 999L)
            assertEquals(product, roundTrip(codecFor(registry, loader, "com.example.Product"), product))
        }
    }

    private fun writeProjectBuildFiles(projectDir: File) {
        projectDir.resolve("settings.gradle.kts").writeText(
            """
            pluginManagement {
                repositories {
                    gradlePluginPortal()
                    mavenCentral()
                }
            }
            dependencyResolutionManagement {
                repositories {
                    mavenCentral()
                }
            }
            rootProject.name = "incremental-smoke"
            """.trimIndent() + "\n",
        )
        projectDir.resolve("build.gradle.kts").writeText(
            """
            plugins {
                id("org.jetbrains.kotlin.jvm") version "$kotlinVersion"
                id("com.google.devtools.ksp") version "$kspVersion"
            }

            kotlin {
                jvmToolchain(17)
                coreLibrariesVersion = "$kotlinMinimumVersion"
            }

            dependencies {
                implementation(files(${kotlinString(coreJar.absolutePath)}))
                ksp(files(${kotlinString(kspJar.absolutePath)}))
            }
            """.trimIndent() + "\n",
        )
    }

    private fun runGradle(projectDir: File, vararg args: String): BuildResult =
        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments(*args, "--stacktrace")
            .build()

    private fun runGradleExpectFailure(projectDir: File, vararg args: String): BuildResult =
        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments(*args, "--stacktrace")
            .buildAndFail()

    private fun snapshotKspOutputs(kspOutputDir: File): Map<String, String> =
        kspOutputDir.walkTopDown()
            .filter { it.isFile }
            .associate { it.relativeTo(kspOutputDir).invariantSeparatorsPath to it.readText() }
            .toSortedMap()

    /** Copies the current sources into a separate clean directory, runs a clean build, and asserts identical KSP outputs. */
    private fun assertMatchesCleanBuild(projectDir: File, incrementalSnapshot: Map<String, String>) {
        val cleanDir = workspace.resolve("clean-${System.nanoTime()}").apply { mkdirs() }
        writeProjectBuildFiles(cleanDir)
        projectDir.resolve("src").copyRecursively(cleanDir.resolve("src"), overwrite = true)
        runGradle(cleanDir, "clean", "classes")
        val cleanSnapshot = snapshotKspOutputs(cleanDir.resolve("build/generated/ksp/main"))
        assertEquals(cleanSnapshot, incrementalSnapshot, "Incremental KSP outputs differed from a clean build")
    }

    private fun withCompiledClassLoader(projectDir: File, block: (ClassLoader) -> Unit) {
        val urls = arrayOf(
            projectDir.resolve("build/classes/kotlin/main").toURI().toURL(),
            projectDir.resolve("build/resources/main").toURI().toURL(),
            projectDir.resolve("build/generated/ksp/main/resources").toURI().toURL(),
        )
        URLClassLoader(urls, KspIncrementalGradleTest::class.java.classLoader).use(block)
    }

    private fun kotlinString(path: String): String =
        "\"" + path.replace("\\", "\\\\").replace("\"", "\\\"").replace("$", "\\$") + "\""
}
