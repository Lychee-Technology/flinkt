package neg
import org.apache.flink.streaming.api.datastream.DataStream
import spike.subtype.SubStream
class Exactly<T>
fun <T> exactTypeOf(@Suppress("UNUSED_PARAMETER") value: T): Exactly<T> = Exactly()
fun check(sub: SubStream<String>) {
    val e = exactTypeOf(sub)
    val x: Exactly<DataStream<String>> = e   // must fail: SubStream<String> is not exactly DataStream<String>
}
