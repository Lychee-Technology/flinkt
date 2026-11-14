package io.flinkt.flink

import io.flinkt.codegen.FlinkView
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * A Kotlin view of one Flink [SingleOutputStreamOperator], such as the result of `map` or a source. It holds that
 * object and nothing else, and [asFlink] returns it. A `DataStreamSource` enters as this view.
 *
 * It offers the operators of [FlinktDataStream], which call Flink on the object this view holds, and forwards Flink's
 * fluent configuration of an operator, such as `name`, `uid` and `setParallelism`, each returning the view of what
 * Flink returned. Flink keeps some state on this object rather than on its transformation, such as a
 * `forceNonParallel()` flag and the side outputs already requested, and it stays on the object, because the view never
 * builds another one.
 */
@FlinkView(
    SingleOutputStreamOperator::class,
    // The first slice's forward list (docs/architecture.md, "The first slice's forward lists"). It leaves out Flink 2.x's
    // @Experimental enableAsyncState: async state (API v2) is out of scope for #31, and a forwarder would make it Flinkt's
    // API. An operator that had it enabled before .flinkt() keeps it. It leaves out returns too, which would retype the
    // stream after the view gave Flink its type, and getSideOutput, which introduces a type. Each is reached through
    // asFlink().
    forward = [
        "name",
        "uid",
        "setUidHash",
        "setDescription",
        "setParallelism",
        "setMaxParallelism",
        "forceNonParallel",
        "setBufferTimeout",
        "disableChaining",
        "startNewChain",
        "slotSharingGroup",
    ],
)
public class FlinktSingleOutputStreamOperator<T> internal constructor(
    private val operator: SingleOutputStreamOperator<T>,
) : FlinktDataStream<T>(operator), FlinktSingleOutputStreamOperatorForwarders<T> {
    /** The Flink operator this view holds: the instance that entered with `.flinkt()`, or that a Flink call returned. */
    override fun asFlink(): SingleOutputStreamOperator<T> = operator
}
