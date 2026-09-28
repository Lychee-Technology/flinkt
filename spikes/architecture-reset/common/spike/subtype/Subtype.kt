// SPIKE (disposable): minimal version of PR #1's subtype façade, for comparison only.
package spike.subtype

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import spike.types.typeInfo

class SubStream<T>(env: StreamExecutionEnvironment, t: Transformation<T>) : DataStream<T>(env, t) {
    inline fun <reified R> map(crossinline f: (T) -> R): SubOp<R> {
        val op = (this as DataStream<T>).map(MapFunction<T, R> { f(it) }, typeInfo<R>())
        return SubOp(op.executionEnvironment, op.transformation)
    }
}

class SubOp<T>(env: StreamExecutionEnvironment, t: Transformation<T>) : SingleOutputStreamOperator<T>(env, t) {
    override fun name(name: String): SubOp<T> { super.name(name); return this }
    override fun setParallelism(parallelism: Int): SubOp<T> { super.setParallelism(parallelism); return this }
}

fun <T> DataStream<T>.subEnter(): SubStream<T> = SubStream(executionEnvironment, transformation)
fun <T> SingleOutputStreamOperator<T>.subEnterOperator(): SubOp<T> = SubOp(executionEnvironment, transformation)
