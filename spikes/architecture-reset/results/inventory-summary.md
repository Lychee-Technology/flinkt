
## DataStream
- f120: 70 public instance methods; builder=11, deprecated=16, final=1, other=14, sink=16, stream=29, stream:introduces-type=11
- f22: 53 public instance methods; builder=9, deprecated=1, final=1, other=14, sink=9, stream=21, stream:introduces-type=11
- f23: 53 public instance methods; builder=9, deprecated=1, final=1, other=14, sink=9, stream=21, stream:introduces-type=11
- f120 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- f22 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- f23 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- only in f120 vs f22: 18; only in f22: 1
    - f120: addSink(org.apache.flink.streaming.api.functions.sink.SinkFunction<T>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<T>) ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
    - f120: assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<T>) ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
    - f120: iterate() ->  org.apache.flink.streaming.api.datastream.IterativeStream<T>
    - f120: iterate(long) ->  org.apache.flink.streaming.api.datastream.IterativeStream<T>
    - f120: keyBy(int[]) ->  org.apache.flink.streaming.api.datastream.KeyedStream<T, org.apache.flink.api.java.tuple.Tuple>
    - f120: keyBy(java.lang.String[]) ->  org.apache.flink.streaming.api.datastream.KeyedStream<T, org.apache.flink.api.java.tuple.Tuple>
    - f120: partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, int) -> <K> org.apache.flink.streaming.api.datastream.DataStream<T>
    - f120: partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, java.lang.String) -> <K> org.apache.flink.streaming.api.datastream.DataStream<T>
    - f120: sinkTo(org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: sinkTo(org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?>, org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.AllWindowedStream<T, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.AllWindowedStream<T, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: writeAsCsv(java.lang.String) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode, java.lang.String, java.lang.String) -> <X> org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsText(java.lang.String) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsText(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    + f22: addSink(org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<T>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
- only in f22 vs f23: 0; only in f23: 0

## SingleOutputStreamOperator
- f120: 88 public instance methods; builder=11, deprecated=16, final=1, other=15, sink=16, stream=46, stream:introduces-type=12
- f22: 73 public instance methods; builder=9, deprecated=1, final=1, other=15, sink=9, stream=40, stream:introduces-type=12
- f23: 73 public instance methods; builder=9, deprecated=1, final=1, other=15, sink=9, stream=40, stream:introduces-type=12
- f120 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- f22 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- f23 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- only in f120 vs f22: 18; only in f22: 3
    - f120: addSink(org.apache.flink.streaming.api.functions.sink.SinkFunction<T>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<T>) ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
    - f120: assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<T>) ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
    - f120: iterate() ->  org.apache.flink.streaming.api.datastream.IterativeStream<T>
    - f120: iterate(long) ->  org.apache.flink.streaming.api.datastream.IterativeStream<T>
    - f120: keyBy(int[]) ->  org.apache.flink.streaming.api.datastream.KeyedStream<T, org.apache.flink.api.java.tuple.Tuple>
    - f120: keyBy(java.lang.String[]) ->  org.apache.flink.streaming.api.datastream.KeyedStream<T, org.apache.flink.api.java.tuple.Tuple>
    - f120: partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, int) -> <K> org.apache.flink.streaming.api.datastream.DataStream<T>
    - f120: partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, java.lang.String) -> <K> org.apache.flink.streaming.api.datastream.DataStream<T>
    - f120: sinkTo(org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: sinkTo(org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?>, org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.AllWindowedStream<T, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.AllWindowedStream<T, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: writeAsCsv(java.lang.String) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode, java.lang.String, java.lang.String) -> <X> org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsText(java.lang.String) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsText(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    + f22: addMetricVariable(java.lang.String, java.lang.String) ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
    + f22: addSink(org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<T>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    + f22: enableAsyncState() ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
- only in f22 vs f23: 0; only in f23: 0

## KeyedStream
- f120: 98 public instance methods; builder=20, deprecated=23, final=1, other=16, sink=16, stream=46, stream:introduces-type=13
- f22: 80 public instance methods; builder=16, deprecated=4, final=1, other=16, sink=9, stream=39, stream:introduces-type=13
- f23: 80 public instance methods; builder=16, deprecated=4, final=1, other=16, sink=9, stream=39, stream:introduces-type=13
- f120 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- f22 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- f23 final: ['union(org.apache.flink.streaming.api.datastream.DataStream<T>[])']
- only in f120 vs f22: 20; only in f22: 2
    - f120: addSink(org.apache.flink.streaming.api.functions.sink.SinkFunction<T>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<T>) ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
    - f120: assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<T>) ->  org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T>
    - f120: iterate() ->  org.apache.flink.streaming.api.datastream.IterativeStream<T>
    - f120: iterate(long) ->  org.apache.flink.streaming.api.datastream.IterativeStream<T>
    - f120: keyBy(int[]) ->  org.apache.flink.streaming.api.datastream.KeyedStream<T, org.apache.flink.api.java.tuple.Tuple>
    - f120: keyBy(java.lang.String[]) ->  org.apache.flink.streaming.api.datastream.KeyedStream<T, org.apache.flink.api.java.tuple.Tuple>
    - f120: partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, int) -> <K> org.apache.flink.streaming.api.datastream.DataStream<T>
    - f120: partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, java.lang.String) -> <K> org.apache.flink.streaming.api.datastream.DataStream<T>
    - f120: sinkTo(org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: sinkTo(org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?>, org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: timeWindow(org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.WindowedStream<T, KEY, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: timeWindow(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.WindowedStream<T, KEY, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.AllWindowedStream<T, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time) ->  org.apache.flink.streaming.api.datastream.AllWindowedStream<T, org.apache.flink.streaming.api.windowing.windows.TimeWindow>
    - f120: writeAsCsv(java.lang.String) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode, java.lang.String, java.lang.String) -> <X> org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsText(java.lang.String) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    - f120: writeAsText(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    + f22: addSink(org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<T>) ->  org.apache.flink.streaming.api.datastream.DataStreamSink<T>
    + f22: enableAsyncState() ->  org.apache.flink.streaming.api.datastream.KeyedStream<T, KEY>
- only in f22 vs f23: 0; only in f23: 0

## Stream-returning and builder-returning methods, f23
| class | method(params) | returns | final | new element type | TypeInformation overload | decl |
|---|---|---|---|---|---|---|
| DataStream | assignTimestampsAndWatermarks(WatermarkStrategy<T>) | SingleOutputStreamOperator |  |  |  | DataStream |
| DataStream | broadcast() | DataStream |  |  |  | DataStream |
| DataStream | broadcast(MapStateDescriptor<?, ?>[]) | BroadcastStream |  |  |  | DataStream |
| DataStream | coGroup(DataStream<T2>) | CoGroupedStreams |  |  |  | DataStream |
| DataStream | connect(BroadcastStream<R>) | BroadcastConnectedStream |  |  |  | DataStream |
| DataStream | connect(DataStream<R>) | ConnectedStreams |  |  |  | DataStream |
| DataStream | countWindowAll(long) | AllWindowedStream |  |  |  | DataStream |
| DataStream | countWindowAll(long, long) | AllWindowedStream |  |  |  | DataStream |
| DataStream | filter(FilterFunction<T>) | SingleOutputStreamOperator |  |  |  | DataStream |
| DataStream | flatMap(FlatMapFunction<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | flatMap(FlatMapFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | forward() | DataStream |  |  |  | DataStream |
| DataStream | fullWindowPartition() | PartitionWindowedStream |  |  |  | DataStream |
| DataStream | global() | DataStream |  |  |  | DataStream |
| DataStream | join(DataStream<T2>) | JoinedStreams |  |  |  | DataStream |
| DataStream | keyBy(KeySelector<T, K>) | KeyedStream |  | yes | yes | DataStream |
| DataStream | keyBy(KeySelector<T, K>, TypeInformation<K>) | KeyedStream |  | yes | yes | DataStream |
| DataStream | map(MapFunction<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | map(MapFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | partitionCustom(Partitioner<K>, KeySelector<T, K>) | DataStream |  |  |  | DataStream |
| DataStream | process(ProcessFunction<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | process(ProcessFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | project(int[]) | SingleOutputStreamOperator |  | yes | no | DataStream |
| DataStream | rebalance() | DataStream |  |  |  | DataStream |
| DataStream | rescale() | DataStream |  |  |  | DataStream |
| DataStream | shuffle() | DataStream |  |  |  | DataStream |
| DataStream | transform(java.lang.String, TypeInformation<R>, OneInputStreamOperator<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | transform(java.lang.String, TypeInformation<R>, OneInputStreamOperatorFactory<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | union(DataStream<T>[]) | DataStream | final |  |  | DataStream |
| DataStream | windowAll(WindowAssigner<? super T, W>) | AllWindowedStream |  |  |  | DataStream |
| SingleOutputStreamOperator | addMetricVariable(java.lang.String, java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | cache() | CachedDataStream |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | disableChaining() | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | enableAsyncState() | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | forceNonParallel() | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | getSideOutput(OutputTag<X>) | SideOutputDataStream |  | yes | no | SingleOutputStreamOperator |
| SingleOutputStreamOperator | name(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | returns(java.lang.Class<T>) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | returns(TypeHint<T>) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | returns(TypeInformation<T>) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setBufferTimeout(long) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setDescription(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setMaxParallelism(int) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setParallelism(int) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setUidHash(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | slotSharingGroup(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | slotSharingGroup(SlotSharingGroup) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | startNewChain() | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | uid(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| KeyedStream | asQueryableState(java.lang.String) | QueryableStateStream |  |  |  | KeyedStream |
| KeyedStream | asQueryableState(java.lang.String, ReducingStateDescriptor<T>) | QueryableStateStream |  |  |  | KeyedStream |
| KeyedStream | asQueryableState(java.lang.String, ValueStateDescriptor<T>) | QueryableStateStream |  |  |  | KeyedStream |
| KeyedStream | countWindow(long) | WindowedStream |  |  |  | KeyedStream |
| KeyedStream | countWindow(long, long) | WindowedStream |  |  |  | KeyedStream |
| KeyedStream | enableAsyncState() | KeyedStream |  |  |  | KeyedStream |
| KeyedStream | flatMap(FlatMapFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | KeyedStream |
| KeyedStream | fullWindowPartition() | PartitionWindowedStream |  |  |  | KeyedStream |
| KeyedStream | intervalJoin(KeyedStream<T1, KEY>) | KeyedStream.IntervalJoin |  |  |  | KeyedStream |
| KeyedStream | max(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | max(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(int, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(java.lang.String, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | min(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | min(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(int, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(java.lang.String, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | process(KeyedProcessFunction<KEY, T, R>) | SingleOutputStreamOperator |  | yes | yes | KeyedStream |
| KeyedStream | process(KeyedProcessFunction<KEY, T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | KeyedStream |
| KeyedStream | reduce(ReduceFunction<T>) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | sum(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | sum(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | window(WindowAssigner<? super T, W>) | WindowedStream |  |  |  | KeyedStream |

## Stream-returning and builder-returning methods, f120
| class | method(params) | returns | final | new element type | TypeInformation overload | decl |
|---|---|---|---|---|---|---|
| DataStream | assignTimestampsAndWatermarks(WatermarkStrategy<T>) | SingleOutputStreamOperator |  |  |  | DataStream |
| DataStream | assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T>) | SingleOutputStreamOperator |  |  |  | DataStream |
| DataStream | assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T>) | SingleOutputStreamOperator |  |  |  | DataStream |
| DataStream | broadcast() | DataStream |  |  |  | DataStream |
| DataStream | broadcast(MapStateDescriptor<?, ?>[]) | BroadcastStream |  |  |  | DataStream |
| DataStream | coGroup(DataStream<T2>) | CoGroupedStreams |  |  |  | DataStream |
| DataStream | connect(BroadcastStream<R>) | BroadcastConnectedStream |  |  |  | DataStream |
| DataStream | connect(DataStream<R>) | ConnectedStreams |  |  |  | DataStream |
| DataStream | countWindowAll(long) | AllWindowedStream |  |  |  | DataStream |
| DataStream | countWindowAll(long, long) | AllWindowedStream |  |  |  | DataStream |
| DataStream | filter(FilterFunction<T>) | SingleOutputStreamOperator |  |  |  | DataStream |
| DataStream | flatMap(FlatMapFunction<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | flatMap(FlatMapFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | forward() | DataStream |  |  |  | DataStream |
| DataStream | fullWindowPartition() | PartitionWindowedStream |  |  |  | DataStream |
| DataStream | global() | DataStream |  |  |  | DataStream |
| DataStream | iterate() | IterativeStream |  |  |  | DataStream |
| DataStream | iterate(long) | IterativeStream |  |  |  | DataStream |
| DataStream | join(DataStream<T2>) | JoinedStreams |  |  |  | DataStream |
| DataStream | keyBy(int[]) | KeyedStream |  |  |  | DataStream |
| DataStream | keyBy(java.lang.String[]) | KeyedStream |  |  |  | DataStream |
| DataStream | keyBy(KeySelector<T, K>) | KeyedStream |  | yes | yes | DataStream |
| DataStream | keyBy(KeySelector<T, K>, TypeInformation<K>) | KeyedStream |  | yes | yes | DataStream |
| DataStream | map(MapFunction<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | map(MapFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | partitionCustom(Partitioner<K>, int) | DataStream |  |  |  | DataStream |
| DataStream | partitionCustom(Partitioner<K>, java.lang.String) | DataStream |  |  |  | DataStream |
| DataStream | partitionCustom(Partitioner<K>, KeySelector<T, K>) | DataStream |  |  |  | DataStream |
| DataStream | process(ProcessFunction<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | process(ProcessFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | project(int[]) | SingleOutputStreamOperator |  | yes | no | DataStream |
| DataStream | rebalance() | DataStream |  |  |  | DataStream |
| DataStream | rescale() | DataStream |  |  |  | DataStream |
| DataStream | shuffle() | DataStream |  |  |  | DataStream |
| DataStream | timeWindowAll(Time) | AllWindowedStream |  |  |  | DataStream |
| DataStream | timeWindowAll(Time, Time) | AllWindowedStream |  |  |  | DataStream |
| DataStream | transform(java.lang.String, TypeInformation<R>, OneInputStreamOperator<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | transform(java.lang.String, TypeInformation<R>, OneInputStreamOperatorFactory<T, R>) | SingleOutputStreamOperator |  | yes | yes | DataStream |
| DataStream | union(DataStream<T>[]) | DataStream | final |  |  | DataStream |
| DataStream | windowAll(WindowAssigner<? super T, W>) | AllWindowedStream |  |  |  | DataStream |
| SingleOutputStreamOperator | cache() | CachedDataStream |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | disableChaining() | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | forceNonParallel() | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | getSideOutput(OutputTag<X>) | SideOutputDataStream |  | yes | no | SingleOutputStreamOperator |
| SingleOutputStreamOperator | name(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | returns(java.lang.Class<T>) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | returns(TypeHint<T>) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | returns(TypeInformation<T>) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setBufferTimeout(long) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setDescription(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setMaxParallelism(int) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setParallelism(int) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | setUidHash(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | slotSharingGroup(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | slotSharingGroup(SlotSharingGroup) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | startNewChain() | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| SingleOutputStreamOperator | uid(java.lang.String) | SingleOutputStreamOperator |  |  |  | SingleOutputStreamOperator |
| KeyedStream | asQueryableState(java.lang.String) | QueryableStateStream |  |  |  | KeyedStream |
| KeyedStream | asQueryableState(java.lang.String, ReducingStateDescriptor<T>) | QueryableStateStream |  |  |  | KeyedStream |
| KeyedStream | asQueryableState(java.lang.String, ValueStateDescriptor<T>) | QueryableStateStream |  |  |  | KeyedStream |
| KeyedStream | countWindow(long) | WindowedStream |  |  |  | KeyedStream |
| KeyedStream | countWindow(long, long) | WindowedStream |  |  |  | KeyedStream |
| KeyedStream | fullWindowPartition() | PartitionWindowedStream |  |  |  | KeyedStream |
| KeyedStream | intervalJoin(KeyedStream<T1, KEY>) | KeyedStream.IntervalJoin |  |  |  | KeyedStream |
| KeyedStream | max(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | max(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(int, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | maxBy(java.lang.String, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | min(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | min(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(int, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | minBy(java.lang.String, boolean) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | process(KeyedProcessFunction<KEY, T, R>) | SingleOutputStreamOperator |  | yes | yes | KeyedStream |
| KeyedStream | process(KeyedProcessFunction<KEY, T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | KeyedStream |
| KeyedStream | process(ProcessFunction<T, R>) | SingleOutputStreamOperator |  | yes | yes | KeyedStream |
| KeyedStream | process(ProcessFunction<T, R>, TypeInformation<R>) | SingleOutputStreamOperator |  | yes | yes | KeyedStream |
| KeyedStream | reduce(ReduceFunction<T>) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | sum(int) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | sum(java.lang.String) | SingleOutputStreamOperator |  |  |  | KeyedStream |
| KeyedStream | timeWindow(Time) | WindowedStream |  |  |  | KeyedStream |
| KeyedStream | timeWindow(Time, Time) | WindowedStream |  |  |  | KeyedStream |
| KeyedStream | window(WindowAssigner<? super T, W>) | WindowedStream |  |  |  | KeyedStream |

## Instance fields (object-local state)
- DataStream f120: DataStream.environment(protected final), DataStream.transformation(protected final)
- DataStream f22: DataStream.environment(protected final), DataStream.transformation(protected final)
- DataStream f23: DataStream.environment(protected final), DataStream.transformation(protected final)
- SingleOutputStreamOperator f120: SingleOutputStreamOperator.nonParallel(protected), SingleOutputStreamOperator.requestedSideOutputs(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- SingleOutputStreamOperator f22: SingleOutputStreamOperator.nonParallel(protected), SingleOutputStreamOperator.requestedSideOutputs(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- SingleOutputStreamOperator f23: SingleOutputStreamOperator.nonParallel(protected), SingleOutputStreamOperator.requestedSideOutputs(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- KeyedStream f120: KeyedStream.keySelector(private final), KeyedStream.keyType(private final), DataStream.environment(protected final), DataStream.transformation(protected final)
- KeyedStream f22: KeyedStream.keySelector(private final), KeyedStream.keyType(private final), KeyedStream.isEnableAsyncState(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- KeyedStream f23: KeyedStream.keySelector(private final), KeyedStream.keyType(private final), KeyedStream.isEnableAsyncState(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- DataStreamSource f120: DataStreamSource.isParallel(private), SingleOutputStreamOperator.nonParallel(protected), SingleOutputStreamOperator.requestedSideOutputs(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- DataStreamSource f22: DataStreamSource.isParallel(private), SingleOutputStreamOperator.nonParallel(protected), SingleOutputStreamOperator.requestedSideOutputs(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- DataStreamSource f23: DataStreamSource.isParallel(private), SingleOutputStreamOperator.nonParallel(protected), SingleOutputStreamOperator.requestedSideOutputs(private), DataStream.environment(protected final), DataStream.transformation(protected final)
- SideOutputDataStream f120: DataStream.environment(protected final), DataStream.transformation(protected final)
- SideOutputDataStream f22: DataStream.environment(protected final), DataStream.transformation(protected final)
- SideOutputDataStream f23: DataStream.environment(protected final), DataStream.transformation(protected final)

## Constructors
- DataStream f120: [] (StreamExecutionEnvironment, Transformation<T>)
- DataStream f22: [] (StreamExecutionEnvironment, Transformation<T>)
- DataStream f23: [] (StreamExecutionEnvironment, Transformation<T>)
- SingleOutputStreamOperator f120: [] (StreamExecutionEnvironment, Transformation<T>)
- SingleOutputStreamOperator f22: [] (StreamExecutionEnvironment, Transformation<T>)
- SingleOutputStreamOperator f23: [] (StreamExecutionEnvironment, Transformation<T>)
- KeyedStream f120: [ package] (DataStream<T>, PartitionTransformation<T>, KeySelector<T, KEY>, TypeInformation<KEY>); [] (DataStream<T>, KeySelector<T, KEY>, TypeInformation<KEY>); [] (DataStream<T>, KeySelector<T, KEY>)
- KeyedStream f22: [ package @Internal] (DataStream<T>, PartitionTransformation<T>, KeySelector<T, KEY>, TypeInformation<KEY>); [] (DataStream<T>, KeySelector<T, KEY>, TypeInformation<KEY>); [] (DataStream<T>, KeySelector<T, KEY>)
- KeyedStream f23: [ package @Internal] (DataStream<T>, PartitionTransformation<T>, KeySelector<T, KEY>, TypeInformation<KEY>); [] (DataStream<T>, KeySelector<T, KEY>, TypeInformation<KEY>); [] (DataStream<T>, KeySelector<T, KEY>)
- DataStreamSource f120: [] (StreamExecutionEnvironment, Source<T, ?, ?>, WatermarkStrategy<T>, TypeInformation<T>, java.lang.String); [] (SingleOutputStreamOperator<T>); [] (StreamExecutionEnvironment, TypeInformation<T>, StreamSource<T, ?>, boolean, java.lang.String, Boundedness); [] (StreamExecutionEnvironment, TypeInformation<T>, StreamSource<T, ?>, boolean, java.lang.String)
- DataStreamSource f22: [] (StreamExecutionEnvironment, Source<T, ?, ?>, WatermarkStrategy<T>, TypeInformation<T>, java.lang.String); [] (SingleOutputStreamOperator<T>); [] (StreamExecutionEnvironment, TypeInformation<T>, StreamSource<T, ?>, boolean, java.lang.String); [] (StreamExecutionEnvironment, TypeInformation<T>, StreamSource<T, ?>, boolean, java.lang.String, Boundedness)
- DataStreamSource f23: [] (StreamExecutionEnvironment, Source<T, ?, ?>, WatermarkStrategy<T>, TypeInformation<T>, java.lang.String); [] (SingleOutputStreamOperator<T>); [] (StreamExecutionEnvironment, TypeInformation<T>, StreamSource<T, ?>, boolean, java.lang.String); [] (StreamExecutionEnvironment, TypeInformation<T>, StreamSource<T, ?>, boolean, java.lang.String, Boundedness)
- SideOutputDataStream f120: [] (StreamExecutionEnvironment, SideOutputTransformation<T>)
- SideOutputDataStream f22: [] (StreamExecutionEnvironment, SideOutputTransformation<T>)
- SideOutputDataStream f23: [] (StreamExecutionEnvironment, SideOutputTransformation<T>)
