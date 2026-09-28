package flinkt.views

/**
 * Marks a view member that calls a Flink method Apache Flink annotates `@Experimental`. Such methods can
 * change or disappear between Flink minor releases, so using one is an explicit choice.
 */
@RequiresOptIn(
    message = "Calls a Flink API that Apache Flink marks @Experimental; it may change between Flink minor releases.",
    level = RequiresOptIn.Level.WARNING,
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.FUNCTION)
public annotation class ExperimentalFlinkApi
