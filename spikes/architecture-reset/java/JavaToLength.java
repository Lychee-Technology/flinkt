package spike.java;
import org.apache.flink.api.common.functions.MapFunction;
/** SPIKE: a Flink function declared in Java; Kotlin sees its output as the platform type Integer!. */
public class JavaToLength implements MapFunction<String, Integer> {
    @Override public Integer map(String value) { return value.length(); }
}
