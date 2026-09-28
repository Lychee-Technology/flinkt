package flinkt.codegen

import kotlin.reflect.KClass

/**
 * Marks a hand-written view over [flinkClass]. The view codegen generates a sealed interface
 * `<ViewName>Forwarders` with one member per public overload of each method in [forward]. A forwarded
 * method calls the Flink method on `asFlink()` and wraps a returned Flink stream in its view. Parameter
 * names come from the names file read from Flink's class files. Flink's `@Deprecated` becomes
 * Kotlin's `@Deprecated`, and Flink's `@Experimental` becomes the `@ExperimentalFlinkApi` opt-in. The
 * codegen fails the build when a listed method would introduce an element type, returns a Flink
 * type that has no view, is `@Internal`, or doesn't exist on the Flink line being compiled.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.SOURCE)
annotation class FlinkView(
    val flinkClass: KClass<*>,
    val forward: Array<String>,
)
