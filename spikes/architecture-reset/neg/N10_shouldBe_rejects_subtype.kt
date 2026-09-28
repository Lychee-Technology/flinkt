package neg
import org.apache.flink.streaming.api.datastream.DataStream
import probes.extra.exactTypeOf
import probes.extra.shouldBe
import spike.subtype.SubStream
fun check(sub: SubStream<String>) = exactTypeOf(sub).shouldBe<DataStream<String>>()   // must fail
