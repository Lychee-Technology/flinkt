package io.flinkt.gradle

import org.gradle.api.GradleException
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.artifacts.result.ResolvedComponentResult
import org.gradle.api.artifacts.result.ResolvedDependencyResult
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.process.CommandLineArgumentProvider

/**
 * The KSP options of `flinkt-view-codegen` (docs/architecture.md, "Parameter names"): the adapter's names file and
 * the version of the Flink on its compile classpath, which the generator checks the file against.
 *
 * KSP's task declares its argument providers as nested inputs, so the names file is a tracked input of the task:
 * editing or extracting it again reruns generation.
 */
public abstract class ViewCodegenArguments : CommandLineArgumentProvider {
    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    public abstract val flinkApiFile: RegularFileProperty

    @get:Input
    public abstract val flinkVersion: Property<String>

    override fun asArguments(): List<String> = listOf(
        "flinkt.view.flinkApiFile=${flinkApiFile.get().asFile.absolutePath}",
        "flinkt.view.flinkVersion=${flinkVersion.get()}",
    )
}

/** What an adapter's generator tests need, passed to the test JVM as system properties. */
public abstract class ViewCodegenTestProperties : CommandLineArgumentProvider {
    /** `flinkt-view-codegen`'s own classes, without their dependencies, which is what KSP needs to run it. */
    @get:Classpath
    public abstract val processorClasspath: ConfigurableFileCollection

    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    public abstract val flinkApiFile: RegularFileProperty

    @get:Input
    public abstract val flinkVersion: Property<String>

    /**
     * The Kotlin that `flinkt-view-codegen` generated in each adapter, this one included: one directory per adapter,
     * which the cross-adapter comparison reads (docs/testing.md, "Generated forwarders").
     */
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    public abstract val adapterForwarders: ConfigurableFileCollection

    override fun asArguments(): List<String> = listOf(
        "-Dflinkt.test.viewCodegen.classpath=${processorClasspath.asPath}",
        "-Dflinkt.test.viewCodegen.flinkApiFile=${flinkApiFile.get().asFile.absolutePath}",
        "-Dflinkt.test.viewCodegen.flinkVersion=${flinkVersion.get()}",
        "-Dflinkt.test.viewCodegen.adapterForwarders=${adapterForwarders.asPath}",
    )
}

/**
 * The arguments of the extraction that writes or checks an adapter's names file from the Flink jars it compiles
 * against (`io.flinkt.codegen.ExtractFlinkApi`).
 */
public abstract class FlinkApiExtractionArguments : CommandLineArgumentProvider {
    /** The Flink jars on the adapter's compile classpath. */
    @get:Classpath
    public abstract val flinkClasspath: ConfigurableFileCollection

    @get:Input
    public abstract val flinkVersion: Property<String>

    /** The Flink classes the views hold. Their supertypes are read too. */
    @get:Input
    public abstract val classes: ListProperty<String>

    /** The names file. An output of the task that writes it, and an input of the task that checks it. */
    @get:Internal
    public abstract val flinkApiFile: RegularFileProperty

    @get:Input
    public abstract val check: Property<Boolean>

    /** The task that writes the file, which a failed check tells the reader to run. */
    @get:Input
    public abstract val extractTask: Property<String>

    override fun asArguments(): List<String> =
        listOf("--flink-version", flinkVersion.get(), "--output", flinkApiFile.get().asFile.absolutePath, "--task", extractTask.get()) +
            (if (check.get()) listOf("--check") else emptyList()) +
            classes.get().flatMap { listOf("--class", it) } +
            flinkClasspath.files.map { it.absolutePath }
}

/** The version that resolution selected for the module [group]:[name] in this graph. */
internal fun ResolvedComponentResult.selectedVersion(group: String, name: String): String {
    val visited = mutableSetOf<ResolvedComponentResult>()
    val queue = ArrayDeque(listOf(this))
    while (queue.isNotEmpty()) {
        val component = queue.removeFirst()
        if (!visited.add(component)) continue
        val id = component.id
        if (id is ModuleComponentIdentifier && id.group == group && id.module == name) return id.version
        component.dependencies.filterIsInstance<ResolvedDependencyResult>().forEach { queue += it.selected }
    }
    throw GradleException("$group:$name isn't on the classpath of ${this.id.displayName}")
}
