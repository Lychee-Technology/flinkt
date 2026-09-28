package neg
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import spike.types.typeInfo
class S<T>(e: StreamExecutionEnvironment, t: Transformation<T>) : DataStream<T>(e, t) {
    override inline fun <reified R> process(fn: ProcessFunction<T, R>): SingleOutputStreamOperator<R> = super.process(fn).returns(typeInfo<R>())
}
