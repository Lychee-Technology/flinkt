package io.flinkt.gradle

import java.io.Serializable
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.artifacts.component.ProjectComponentIdentifier
import org.gradle.api.artifacts.result.ResolvedComponentResult
import org.gradle.api.artifacts.result.ResolvedDependencyResult
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/**
 * One boundary on one classpath of a module.
 *
 * A component violates it if it's a [forbiddenProjects] project or a module of the same name, if its group is in
 * [forbiddenGroups], or, when [allowedModules] is set, if it isn't one of those `group:name` modules.
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
 * (docs/build.md#dependency-boundaries). Each violation names the dependency path that brought the component in.
 */
@DisableCachingByDefault(because = "Reads only the resolved dependency graph and writes nothing")
public abstract class DependencyBoundaryCheck : DefaultTask() {
    @get:Internal
    public abstract val modulePath: Property<String>

    /** The root of each checked classpath's resolved graph, keyed by configuration name. */
    @get:Input
    public abstract val classpaths: MapProperty<String, ResolvedComponentResult>

    @get:Input
    public abstract val rules: ListProperty<BoundaryRule>

    public fun forbid(classpath: String, projects: List<String> = emptyList(), groups: List<String> = emptyList(), reason: String) {
        rules.add(BoundaryRule(classpath, forbiddenProjects = projects, forbiddenGroups = groups, reason = reason))
    }

    public fun allowOnly(classpath: String, modules: List<String>, reason: String) {
        rules.add(BoundaryRule(classpath, allowedModules = modules, reason = reason))
    }

    @TaskAction
    public fun verify() {
        val roots = classpaths.get()
        val violations = rules.get().mapNotNull { rule ->
            val root = roots[rule.classpath]
                ?: throw GradleException("${modulePath.get()}: no classpath '${rule.classpath}' to check")
            val offenders = offenders(root, rule)
            if (offenders.isEmpty()) {
                null
            } else {
                "${rule.classpath}: ${rule.reason}\n" +
                    offenders.joinToString("\n") { (what, path) -> "      $what, through ${path.joinToString(" -> ")}" }
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
}
