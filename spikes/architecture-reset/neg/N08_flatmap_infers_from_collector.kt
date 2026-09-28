package neg
import spike.compose.FlinktStream
// Probe, not a contract: K2 infers R from the out.collect(...) call, so this compiles (R = Int).
fun check(s: FlinktStream<String>) = s.flatMap { v, out -> out.collect(v.length) }
