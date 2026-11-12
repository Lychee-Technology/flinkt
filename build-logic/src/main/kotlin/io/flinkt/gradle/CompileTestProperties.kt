package io.flinkt.gradle

import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.process.CommandLineArgumentProvider

/**
 * What the compile-test harness in `:test-support` needs, passed to a test JVM as system properties.
 * Declared as task inputs, so changing a compiler, KSP or the module reruns the tests.
 */
public abstract class CompileTestProperties : CommandLineArgumentProvider {
    @get:Input
    public abstract val buildCompilerVersion: Property<String>

    @get:Classpath
    public abstract val buildCompilerClasspath: ConfigurableFileCollection

    @get:Input
    public abstract val minimumCompilerVersion: Property<String>

    @get:Classpath
    public abstract val minimumCompilerClasspath: ConfigurableFileCollection

    @get:Input
    public abstract val kspVersion: Property<String>

    @get:Classpath
    public abstract val kspClasspath: ConfigurableFileCollection

    /** The module as a consumer compiles against it, plus `:test-support` for `exactTypeOf`. */
    @get:Classpath
    public abstract val moduleClasspath: ConfigurableFileCollection

    /** Where each compilation writes its sources, generated code and classes. Kept for inspecting a failure. */
    @get:Internal
    public abstract val workDir: DirectoryProperty

    override fun asArguments(): List<String> = listOf(
        "-Dflinkt.compileTest.kotlin.build.version=${buildCompilerVersion.get()}",
        "-Dflinkt.compileTest.kotlin.build.classpath=${buildCompilerClasspath.asPath}",
        "-Dflinkt.compileTest.kotlin.minimum.version=${minimumCompilerVersion.get()}",
        "-Dflinkt.compileTest.kotlin.minimum.classpath=${minimumCompilerClasspath.asPath}",
        "-Dflinkt.compileTest.ksp.version=${kspVersion.get()}",
        "-Dflinkt.compileTest.ksp.classpath=${kspClasspath.asPath}",
        "-Dflinkt.compileTest.module.classpath=${moduleClasspath.asPath}",
        "-Dflinkt.compileTest.workDir=${workDir.get().asFile.absolutePath}",
    )
}
