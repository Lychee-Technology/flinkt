package io.flinkt.codegen

import kotlin.reflect.KClass

/**
 * Marks a view over [flinkClass], and lists the Flink methods it forwards.
 *
 * `flinkt-view-codegen` generates a `sealed interface <View>Forwarders` in the view's package, which the view
 * implements. It has `asFlink()`, which the view overrides to return the object it holds, and one member for each
 * public overload of each method in [forward], as Flink declares it on the adapter's line: its overloads, its
 * parameter names from the adapter's `flink-api.tsv`, `@Deprecated`, and `@Experimental` as `@ExperimentalFlinkApi`.
 * A member makes one call to the Flink method on `asFlink()`, and wraps a returned stream that has a view in that
 * view, with the view's one-argument constructor.
 *
 * The build fails when a listed method is missing on the line, is `@Internal`, returns a type built from one of its
 * own type parameters, or returns a Flink stream type that has no view.
 *
 * When a superclass of [flinkClass] has a view too, the generated interface extends that view's, and [forward] lists
 * only methods that [flinkClass] or a class below that superclass declares. Each method is forwarded by the view of
 * the class that declares it, so every view below gets it.
 *
 * The annotation has source retention: it never reaches a class file, and this module never reaches a published
 * runtime classpath. See docs/architecture.md, "Generated forwarders".
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.SOURCE)
public annotation class FlinkView(
    /** The Flink class whose object the view holds. */
    val flinkClass: KClass<*>,
    /** The names of the Flink methods the view forwards. A name covers all of its public overloads. */
    val forward: Array<String>,
)
