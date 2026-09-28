package probes.graph

fun lineSpecific() = println("| 1.20 has no enableAsyncState | n/a | |")

fun nonParallelSource(e: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment) =
    e.addSource(object : org.apache.flink.streaming.api.functions.source.SourceFunction<Long> {
        override fun run(ctx: org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Long>) {}
        override fun cancel() {}
    }, org.apache.flink.api.common.typeinfo.Types.LONG)
