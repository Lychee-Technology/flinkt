package io.flinkt

/**
 * Marks a class that Flinkt models with generated code rather than Flink's type extraction.
 *
 * `flinkt-ksp` is to generate the class's codec, persisted schema and migration constructor
 * (README, "Compile-time generated types"), and a `GeneratedTypeModule` that registers them. It doesn't exist yet.
 *
 * The annotation is kept at runtime. A consumer module's KSP has to recognize it on a class from a dependency jar,
 * which source retention wouldn't allow, and type resolution reads it to tell an annotated class whose generated
 * module wasn't found from a class that isn't annotated at all.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
public annotation class FlinkType
