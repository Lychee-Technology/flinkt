package neg
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import spike.types.typeInfo
class S<T>(e: StreamExecutionEnvironment, t: Transformation<T>) : DataStream<T>(e, t) {
    inline fun <reified R> map(fn: MapFunction<T, R>): SingleOutputStreamOperator<R> = (this as DataStream<T>).map(fn, typeInfo<R>())
}
