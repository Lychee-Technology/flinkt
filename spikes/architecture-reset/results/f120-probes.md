## probes.ResolutionKt
| case | exact static type | output TypeInformation Flink holds |
|---|---|---|
| compose: s.map { User(...) } | `spike.compose.FlinktOperator<spike.model.User!>` | GeneratedTypeInfo<User> |
| compose: s.map(MapFunction<String, User> { ... }) | `spike.compose.FlinktOperator<spike.model.User>` | GeneratedTypeInfo<User> |
| compose: s.map(ToUser()) | `spike.compose.FlinktOperator<spike.model.User>` | GeneratedTypeInfo<User> |
| compose: s.map(ToUserRich()) | `spike.compose.FlinktOperator<spike.model.User!>` | GeneratedTypeInfo<User> |
| compose: s.map(JavaToLength()) | `spike.compose.FlinktOperator<java.lang.Integer!>` | Integer |
| compose: s.map { it.length } | `spike.compose.FlinktOperator<java.lang.Integer!>` | Integer |
| compose: s.map { it.takeIf { false } } (String?) | `n/a` | THREW UnsupportedFlinktType: java.lang.String? is nullable; the spike models only non-null types |
| compose: s.process(MaybeUser()) (User?) | `n/a` | THREW UnsupportedFlinktType: spike.model.User? is nullable; the spike models only non-null types |
| compose: s.process(Passthrough()) | `spike.compose.FlinktOperator<java.lang.String>` | String |
| compose: map(ToUser()).keyBy { it.id } | `spike.compose.FlinktKeyedStream<spike.model.User, java.lang.Long!>` | Long |
| compose: keyed.process(SessionFunction()) | `spike.compose.FlinktOperator<spike.model.Session>` | GeneratedTypeInfo<Session> |
| compose: keyBy(KeySelector<User, Long> { it.id }) | `spike.compose.FlinktKeyedStream<spike.model.User, java.lang.Long>` | Long |
| compose: s.flatMap<Int> { v, out -> out.collect(v.length) } | `spike.compose.FlinktOperator<java.lang.Integer>` | Integer |
| compose: s.union(s, s) | `spike.compose.FlinktStream<java.lang.String>` | String |
| compose: s.map { ... }.name("n").uid("u").setParallelism(2) | `spike.compose.FlinktOperator<spike.model.User>` | - |
| compose: s.asFlink().map { User(...) } (explicit exit) | `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<spike.model.User!>!` | GenericType<spike.model.User> |
| compose: raw.map(ToUser()).flinkt() | `spike.compose.FlinktOperator<spike.model.User!>` | - |
| compose: keyedFlink.flinkt() | `spike.compose.FlinktKeyedStream<java.lang.String!, java.lang.String!>` | - |
| subtype: sub.map { User(...) } | `spike.subtype.SubOp<spike.model.User>` | GeneratedTypeInfo<User> |
| subtype: sub.map(MapFunction<String, User> { ... }) | `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<spike.model.User!>!` | GenericType<spike.model.User> |
| subtype: sub.map(ToUser()) | `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<spike.model.User!>!` | GenericType<spike.model.User> |
| subtype: sub.union(sub) | `org.apache.flink.streaming.api.datastream.DataStream<java.lang.String!>!` | String |
| subtype: sub.keyBy { it } | `org.apache.flink.streaming.api.datastream.KeyedStream<java.lang.String!, java.lang.String!>!` | String |
expectType<DataStream<String>>(sub) compiled; staticType(sub) = spike.subtype.SubStream<java.lang.String>; staticType(sub.union(sub)) = org.apache.flink.streaming.api.datastream.DataStream<java.lang.String!>!
identity: s.map(...).asFlink() is Flink's own object: org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
## probes.positive.PositiveKt
flatMap without type argument: static spike.compose.FlinktOperator<java.lang.Integer!>; Flink type Integer
valueStateDescriptor<User>: org.apache.flink.api.common.state.ValueStateDescriptor<spike.model.User>; serializer type VALUE
exactTypeOf(sub.union(sub)) accepted as Exactly<DataStream<String>>: Exactly
ext: raw.map { it.length } resolves to: org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<java.lang.Integer!>!
ext: raw.map(kotlinFn = { it.length }) resolves to: java.lang.String
## probes.graph.GraphKt
| check | result | detail |
|---|---|---|
| milestone chain: same DAG as direct Flink (types, partitioner, uid, parallelism) | PASS | SourceTransformation[name=Collection Source uid=null p=1 maxP=-1 out=GeneratedTypeInfo<UserEvent>   inputs=0] -> OneInputTransformation[name=users uid=users-v1 p=4 maxP=-1 out=GeneratedTypeInfo<User>  StreamMap inputs=1] -> PartitionTransformation[name=Partition uid=null p=4 maxP=-1 out=GeneratedTypeInfo<User> KeyGroupStreamPartitioner  inputs=1] -> OneInputTransformation[name=KeyedProcess uid=null p=4 maxP=-1 out=GeneratedTypeInfo<Session>  KeyedProcessOperator inputs=1] |
| op.flinkt().asFlink() === op | PASS |  |
| keyed.flinkt().asFlink() === keyed | PASS |  |
| entering adds no transformation | PASS |  |
| forceNonParallel then setParallelism(2): view behaves as Flink | PASS | flink: rejected: IllegalArgumentException: The parallelism of non parallel operator must be 1.; view: rejected: IllegalArgumentException: The parallelism of non parallel operator must be 1. |
| forceNonParallel then setParallelism(2): subtype rebuild behaves as Flink | FAIL | subtype rebuild: accepted |
| side output id reused with another type after view request: rejected as in Flink | PASS | rejected: UnsupportedOperationException: A side output with a matching id was already requested with a different type. This is not  |
| non-parallel source: view setParallelism(2) rejected as in Flink | PASS | rejected: IllegalArgumentException: The parallelism of non parallel operator must be 1. |
| reinterpretAsKeyedStream: view DAG == direct | PASS | PartitionTransformation[name=Partition uid=null p=4 maxP=-1 out=GeneratedTypeInfo<User> ForwardPartitioner  inputs=1] |
| reinterpretAsKeyedStream: subtype rebuild DAG == direct | FAIL | PartitionTransformation[name=Partition uid=null p=4 maxP=-1 out=GeneratedTypeInfo<User> ForwardPartitioner  inputs=1], PartitionTransformation[name=Partition uid=null p=4 maxP=-1 out=GeneratedTypeInfo<User> KeyGroupStreamPartitioner  inputs=1] |
| union through the view == direct | PASS |  |
| side output view carries the tag's TypeInformation | PASS | GeneratedTypeInfo<UserEvent> |
| 1.20 has no enableAsyncState | n/a | |
## probes.runtime.RuntimeKt
MiniCluster result (generic types disabled): [Session(userId=1, count=1), Session(userId=1, count=2), Session(userId=1, count=3), Session(userId=2, count=1)]
snapshot restore: compatibility=true value=User(id=7, name=x)
## probes.safety.SafetyNetKt
raw map output type: GenericType<spike.model.User>
execute with generic types disabled: UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig and type spike.model.User is treated as a generic type.
## probes.extra.ExtraKt
KType equality on SAM-lambda result: false (spike.compose.FlinktOperator<spike.model.User!>)
raw Flink map(fn implementing ResultTypeQueryable): GeneratedTypeInfo<User>
explicit nullable result for a Java function: java.lang.Integer? is nullable; the spike models only non-null types
