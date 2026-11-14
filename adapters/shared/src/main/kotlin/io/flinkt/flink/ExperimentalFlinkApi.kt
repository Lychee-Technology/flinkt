package io.flinkt.flink

/**
 * Marks Flinkt API that calls a Flink method Apache Flink annotates `@Experimental`.
 *
 * Flink can change or remove such a method in any release, so Flinkt's API built on it is explicitly experimental too
 * (docs/flink-compatibility.md, "@Experimental"). A view's forwarder of an `@Experimental` Flink method carries this
 * marker, as would one of `enableAsyncState()` on 2.x, which no view forwards. A call without
 * `@OptIn(ExperimentalFlinkApi::class)` compiles with a warning, as using the Flink method is a choice Flink asks
 * callers to make knowingly.
 *
 * It has the same fully-qualified name on every adapter, so an `@OptIn` doesn't change with the Flink line. Its
 * targets are those of Flink's own `@Experimental`.
 */
@RequiresOptIn(
    message = "This calls a Flink API that Apache Flink marks @Experimental, which can change or go away in any Flink release.",
    level = RequiresOptIn.Level.WARNING,
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY, AnnotationTarget.CONSTRUCTOR)
@MustBeDocumented
public annotation class ExperimentalFlinkApi
