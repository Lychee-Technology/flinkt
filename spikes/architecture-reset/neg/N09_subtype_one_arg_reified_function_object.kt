package neg
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import spike.types.typeInfo
// A keyed subtype would need this to type process(SessionFunction()); there is no legal spelling.
abstract class K<T, KEY>(d: org.apache.flink.streaming.api.datastream.DataStream<T>, s: org.apache.flink.api.java.functions.KeySelector<T, KEY>) : KeyedStream<T, KEY>(d, s) {
    inline fun <reified R> process(fn: KeyedProcessFunction<KEY, T, R>): SingleOutputStreamOperator<R> = super.process(fn).returns(typeInfo<R>())
}
