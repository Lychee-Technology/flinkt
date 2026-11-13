package io.flinkt.flink

import io.flinkt.codegen.FlinkView
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * A Kotlin view of one Flink [SingleOutputStreamOperator], such as the result of `map` or a source. It holds that
 * object and nothing else, and [asFlink] returns it. A `DataStreamSource` enters as this view.
 *
 * It offers the operators of [FlinktDataStream], which call Flink on the object this view holds. Flink keeps some
 * state on this object rather than on its transformation, such as a `forceNonParallel()` flag and the side outputs
 * already requested, and it stays on the object, because the view never builds another one.
 */
@FlinkView(SingleOutputStreamOperator::class, forward = [])
public class FlinktSingleOutputStreamOperator<T> internal constructor(
    private val operator: SingleOutputStreamOperator<T>,
) : FlinktDataStream<T>(operator), FlinktSingleOutputStreamOperatorForwarders<T> {
    /** The Flink operator this view holds: the instance that entered with `.flinkt()`, or that a Flink call returned. */
    override fun asFlink(): SingleOutputStreamOperator<T> = operator
}
