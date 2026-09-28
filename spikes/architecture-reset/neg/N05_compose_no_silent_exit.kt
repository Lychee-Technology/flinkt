package neg
import spike.compose.FlinktStream
fun check(s: FlinktStream<String>, t: FlinktStream<Int>) {
    s.connect(t.asFlink())   // must fail: the view has no connect; the exit has to be written as asFlink()
}
