package io.flinkt

/**
 * Marks a class that Flinkt models with generated code rather than Flink's type extraction.
 *
 * `flinkt-ksp` is to generate the class's codec, persisted schema and migration constructor
 * (README, "Compile-time generated types"). It doesn't exist yet, so for now the annotation only marks the class.
 */
@Target(AnnotationTarget.CLASS)
@MustBeDocumented
public annotation class FlinkType
