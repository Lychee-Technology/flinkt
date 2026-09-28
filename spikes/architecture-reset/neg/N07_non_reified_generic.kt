package neg
import org.apache.flink.streaming.api.functions.ProcessFunction
import spike.compose.FlinktStream
fun <R> check(s: FlinktStream<String>, fn: ProcessFunction<String, R>) = s.process(fn)   // must fail: R is not known here
