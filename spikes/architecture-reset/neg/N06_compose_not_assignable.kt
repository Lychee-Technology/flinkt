package neg
import org.apache.flink.streaming.api.datastream.DataStream
import spike.compose.FlinktStream
fun check(s: FlinktStream<String>) {
    val d: DataStream<String> = s   // must fail: a view is not a DataStream; interop is s.asFlink()
}
