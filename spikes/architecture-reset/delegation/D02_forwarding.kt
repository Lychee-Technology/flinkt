// SPIKE: the closest thing to delegation a Flink class allows: a subclass that forwards calls by hand.
package deleg

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.PartitionTransformation

// Forwards setParallelism; setMaxParallelism stands for any method that was missed or added by a later Flink release.
class ForwardingOperator<T>(val original: SingleOutputStreamOperator<T>) :
    SingleOutputStreamOperator<T>(original.executionEnvironment, original.transformation) {
    override fun setParallelism(parallelism: Int): ForwardingOperator<T> { original.setParallelism(parallelism); return this }
    override fun forceNonParallel(): ForwardingOperator<T> { original.forceNonParallel(); return this }
}

// A KeyedStream subclass must call a public KeyedStream constructor, which builds a new PartitionTransformation.
class ForwardingKeyed<T, K>(val original: KeyedStream<T, K>) :
    KeyedStream<T, K>(original, original.keySelector, original.keyType) {
    override fun getTransformation(): Transformation<T> = original.transformation
}

fun partitions(t: Transformation<*>): Int =
    (if (t is PartitionTransformation<*>) 1 else 0) + t.inputs.sumOf { partitions(it) }

fun outcome(block: () -> Any?) = runCatching { block(); "accepted" }.getOrElse { "rejected (${it.javaClass.simpleName})" }

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val src: DataStream<Long> = env.fromData(listOf(1L, 2L), Types.LONG)

    val op = src.map(MapFunction<Long, Long> { it + 1 }, Types.LONG)
    val fwd = ForwardingOperator(op)
    op.forceNonParallel()
    println("objects: fwd === op? ${fwd === op}; fwd is a DataStream: ${fwd is DataStream<*>}")
    println("original.setMaxParallelism(2): ${outcome { op.setMaxParallelism(2) }}")
    println("forwarded  fwd.setParallelism(2): ${outcome { fwd.setParallelism(2) }}")
    println("not forwarded fwd.setMaxParallelism(2): ${outcome { fwd.setMaxParallelism(2) }}")

    val keyed = src.keyBy(KeySelector<Long, Long> { it }, Types.LONG)
    val other = env.fromData(listOf(3L), Types.LONG)
    println("partition steps, keyed.union(other): ${partitions(keyed.union(other).transformation)}")
    println("partition steps, ForwardingKeyed(keyed).union(other) (union is final, reads this.transformation): ${partitions(ForwardingKeyed(keyed).union(other).transformation)}")
}
