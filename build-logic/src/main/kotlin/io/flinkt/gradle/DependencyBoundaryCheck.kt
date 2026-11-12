package io.flinkt.gradle

import java.io.Serializable
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.NamedDomainObjectProvider
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.artifacts.component.ProjectComponentIdentifier
import org.gradle.api.artifacts.result.ResolvedComponentResult
import org.gradle.api.artifacts.result.ResolvedDependencyResult
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/**
 * One boundary on one classpath of a module.
 *
 * A component violates it if it's a [forbiddenProjects] project or a module of the same name, if its group is in
 * [forbiddenGroups], or, when [allowedModules] is set, if it isn't one of those `group:name` modules. A file
 * dependency violates every rule, because it has no coordinates for a rule to check.
 */
public data class BoundaryRule(
    val classpath: String,
    val forbiddenProjects: List<String> = emptyList(),
    val forbiddenGroups: List<String> = emptyList(),
    val allowedModules: List<String>? = null,
    val reason: String,
) : Serializable

/**
 * Fails when a module's resolved classpath crosses one of the architecture's dependency boundaries
 * (docs/build.md#dependency-boundaries). Each violation names the dependency path that brought the component in, or,
 * for a file dependency, which isn't in the dependency graph, the file.
 */
@DisableCachingByDefault(because = "Reads only the resolved dependencies and writes nothing")
public abstract class DependencyBoundaryCheck : DefaultTask() {
    @get:Internal
    public abstract val modulePath: Property<String>

    /** The root of each checked classpath's resolved graph, keyed by configuration name. */
    @get:Input
    public abstract val classpaths: MapProperty<String, ResolvedComponentResult>

    /**
     * The file dependencies on each checked classpath, keyed by configuration name: each file that a `files(...)`
     * dependency of the module, or of a project it depends on, puts there. Gradle leaves them out of the resolved
     * graph, so [classpaths] doesn't show them.
     */
    @get:Input
    public abstract val fileDependencies: MapProperty<String, List<String>>

    @get:Input
    public abstract val rules: ListProperty<BoundaryRule>

    /** Checks the classpath that [configuration] resolves: its dependency graph and its file dependencies. */
    public fun classpath(configuration: NamedDomainObjectProvider<Configuration>) {
        classpaths.put(configuration.name, configuration.flatMap { it.incoming.resolutionResult.rootComponent })
        fileDependencies.put(configuration.name, configuration.flatMap(::filesOutsideTheGraph))
    }

    public fun forbid(classpath: String, projects: List<String> = emptyList(), groups: List<String> = emptyList(), reason: String) {
        rules.add(BoundaryRule(classpath, forbiddenProjects = projects, forbiddenGroups = groups, reason = reason))
    }

    public fun allowOnly(classpath: String, modules: List<String>, reason: String) {
        rules.add(BoundaryRule(classpath, allowedModules = modules, reason = reason))
    }

    @TaskAction
    public fun verify() {
        val roots = classpaths.get()
        val files = fileDependencies.get()
        val violations = rules.get().mapNotNull { rule ->
            val root = roots[rule.classpath]
                ?: throw GradleException("${modulePath.get()}: no classpath '${rule.classpath}' to check")
            val offenders = offenders(root, rule).map { (what, path) -> "$what, through ${path.joinToString(" -> ")}" } +
                files[rule.classpath].orEmpty().map { fileViolation(rule, it) }
            if (offenders.isEmpty()) {
                null
            } else {
                "${rule.classpath}: ${rule.reason}\n" + offenders.joinToString("\n") { "      $it" }
            }
        }
        if (violations.isNotEmpty()) {
            throw GradleException(
                "Dependency boundary violated in ${modulePath.get()}:\n" + violations.joinToString("\n") { "  - $it" },
            )
        }
    }

    private fun violation(rule: BoundaryRule, component: ResolvedComponentResult): String? =
        when (val id = component.id) {
            is ProjectComponentIdentifier -> when {
                id.projectPath in rule.forbiddenProjects -> "project ${id.projectPath}"
                rule.allowedModules != null -> "project ${id.projectPath}, which isn't one of ${rule.allowedModules}"
                else -> null
            }
            is ModuleComponentIdentifier -> {
                val module = "${id.group}:${id.module}"
                when {
                    rule.forbiddenProjects.any { it.substringAfterLast(':') == id.module } -> "$module:${id.version}"
                    id.group in rule.forbiddenGroups -> "$module:${id.version}"
                    rule.allowedModules != null && module !in rule.allowedModules ->
                        "$module:${id.version}, which isn't one of ${rule.allowedModules}"
                    else -> null
                }
            }
            else -> null
        }

    /**
     * A file dependency has no group or name, so no rule can clear it. Nor is it in the graph, so there's no path to
     * name: it was declared with `files(...)` in this module or in a project on the path.
     */
    private fun fileViolation(rule: BoundaryRule, file: String): String {
        val what = if (rule.allowedModules != null) "which isn't one of ${rule.allowedModules}" else "which has no coordinates to check"
        return "file dependency $file, $what, declared with files() in this module or in a project it depends on"
    }

    /**
     * The components on the classpath that violate [rule], each with the shortest dependency path to it. The search
     * doesn't descend into an offender, so what it brought in isn't reported separately.
     */
    private fun offenders(root: ResolvedComponentResult, rule: BoundaryRule): List<Pair<String, List<String>>> {
        val offenders = mutableListOf<Pair<String, List<String>>>()
        val queue = ArrayDeque(listOf(root to listOf(root.id.displayName)))
        val seen = mutableSetOf(root)
        while (queue.isNotEmpty()) {
            val (component, path) = queue.removeFirst()
            for (dependency in component.dependencies.filterIsInstance<ResolvedDependencyResult>()) {
                val selected = dependency.selected
                if (!seen.add(selected)) continue
                val next = path + selected.id.displayName
                val what = violation(rule, selected)
                if (what != null) offenders += what to next else queue.addLast(selected to next)
            }
        }
        return offenders
    }

    private companion object {
        /**
         * The files on [configuration]'s classpath that come from no module and no project: its file dependencies.
         * The view selects only those, so the check neither downloads the modules' jars nor builds the projects'.
         */
        fun filesOutsideTheGraph(configuration: Configuration): Provider<List<String>> =
            configuration.incoming
                .artifactView {
                    componentFilter { it !is ModuleComponentIdentifier && it !is ProjectComponentIdentifier }
                }
                .artifacts.resolvedArtifacts
                .map { artifacts -> artifacts.map { it.file.path } }
    }
}
