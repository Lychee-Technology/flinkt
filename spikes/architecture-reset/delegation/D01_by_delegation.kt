import org.apache.flink.streaming.api.datastream.DataStream
class FlinktDataStream<T>(original: DataStream<T>) : DataStream<T> by original
