# Architecture

This document records how Flinkt is built and why. The [README](../README.md) describes what users see. [flink-compatibility.md](flink-compatibility.md) sets the Flink version policy, and [testing.md](testing.md) defines the evidence each claim needs.

## Requirements

The architecture has to preserve these:

- **Flink's vocabulary.** Operators keep Flink's names (`map`, `filter`, `keyBy`, `process`, `name`, `uid`, `setParallelism`). There's no parallel set of names such as `mapK`.
- **Complete Kotlin types.** Every Flinkt call that introduces an element type gives Flink an explicit `TypeInformation` for the full Kotlin type, including nested generic arguments and nullability.
- **No silent Kryo.** A type Flinkt can't model fails explicitly. Generic serialization happens only when the user passes it explicitly.
- **No reflection on the per-record path**: serialize, deserialize, copy, field access, and comparison.
- **Flink's semantics unchanged.** That covers execution, partitioning, validation, state, checkpoints, and savepoints. Using Flinkt changes nothing Flink sees.
- **Explicit failure.** Unsupported behavior fails instead of being approximated.
- **Version isolation.** Flink-facing code lives in per-line adapters, and the application API is the same on every adapter.
- **Interoperability.** Every Flink API stays usable with a stream that went through Flinkt.
- **Strict persisted-state compatibility**, backed by old-bytes and savepoint evidence.

Two things that looked like requirements are preferences, and this design gives them up. One is that a Flinkt stream is directly assignable to `DataStream<T>`. The other is that leaving Flinkt needs no syntax at all. Both would come at the expense of the requirements above, as the next section shows.

## Decision: streams are views, not Flink subtypes

A Flinkt stream is a view that holds exactly one Flink stream object. `FlinktDataStream<T>` holds a `DataStream<T>`, `FlinktSingleOutputStreamOperator<T>` holds a `SingleOutputStreamOperator<T>`, and `FlinktKeyedStream<T, K>` holds a `KeyedStream<T, K>`. A view doesn't extend the Flink class. The README states [the rules users rely on](../README.md#flinkt-streams). The [implementation rules](#views) below keep them true.

The first version of this design, in PR #1, made each Flinkt stream a subclass of the Flink class it stood for (`FlinktDataStream<T> : DataStream<T>`). Five reviews then found a string of problems, and each fix added an exception to the model: existing keyed streams rejected, adapted operators losing their configuration methods, `union` as a forced exit, a per-line list of exits, and function-object operators without a result type ([#3](https://github.com/Lychee-Technology/flinkt/issues/3)). The [architecture review](https://github.com/Lychee-Technology/flinkt/pull/1) traced them to two questions the subtype design couldn't answer well:

- **Which object owns a stream's Flink state?** A subtype can't *be* the object Flink created, so it has to build a second one, on entry and around every object a Flink call returns. Flink keeps some state on stream objects rather than on their transformations: a keyed stream's partitioning, `forceNonParallel()`, requested side outputs, async state in 2.x, and a source's parallelism. A second object doesn't have that state.
- **Which calls are Flinkt's?** A subtype inherits every Flink method under the same name. Whether a call gets Kotlin types then depends on details the user can't see at the call site: lambda versus function object, `final` versus overridable, whether an override exists, and whether an override could be reified at all.

The following were executed against Flink 1.20.5, 2.2.1 and 2.3.0 with Kotlin 2.4.20 ([spike](https://github.com/Lychee-Technology/flinkt/tree/dd2d0f18f6d683107c5ff289c2e6d8bf93d17029/spikes/architecture-reset)):

- **Rebuilt objects lost state.** A subtype rebuilt over a configured operator accepted `setParallelism(2)` after `forceNonParallel()`. A rebuilt keyed stream added a hash shuffle after `reinterpretAsKeyedStream`, and on 2.x it dropped `enableAsyncState()`.
- **Inherited calls fell back to Kryo.** `map(MapFunction { … })` and `map(ToUser())` on the subtype resolved to Flink's inherited `map`, and Flink typed the result `GenericType<User>`, which is Kryo.
- **A reified override doesn't compile.** Kotlin rejects it ("override by a function with reified type parameter"). A member with Flink's signature that isn't marked `override` doesn't compile either ("hides member of supertype").
- **The subtype needed long hand-kept lists.** To keep a chain in the façade, it had to override 97 stream-returning methods covariantly in 2.3 and 118 in 1.20. Another 34 builder-returning methods (42 in 1.20) were exits, and `union` is `final`. Omitting one of these was silent.
- **The views passed every check.** The view kept every check, built the same graph as direct Flink calls, and gave every lambda and function-object form the right `TypeInformation`.

Giving up assignability costs one call, `asFlink()`, wherever a Flink API needs the Flink object.

### Alternatives

- **Kotlin delegation, or a subclass that forwards by hand.** `class FlinktDataStream<T>(d: DataStream<T>) : DataStream<T> by d` doesn't compile, because Kotlin delegates only to interfaces ("delegation is supported only for interfaces"). `DataStream`, `SingleOutputStreamOperator` and `KeyedStream` implement no interface on any target line. The nearest substitute is a subclass that overrides each method to call the original, and it is still a second Flink object. A method it doesn't forward acts on the subclass's own fields: after `forceNonParallel()` on the original, the subclass accepted `setMaxParallelism(2)`. `union` is `final` and reads `this.transformation`, so a forwarding keyed subclass added a second partition step. Both results were executed on 1.20 and 2.3. The view keeps the goal of forwarding, one Flink object and calls reaching it, without the subclass. [Generated forwarders](#generated-forwarders) remove the hand-written forwarding.
- **Subtypes for some classes, views for the others.** Every split tried keeps both problems for the subtype half, and users would have two rules to learn.
- **Extension functions only.** An extension named `map` loses to Flink's member `map`. It's reachable only with a named argument, as in `raw.map(kotlinFn = { … })`. Anything shorter needs new operator names.
- **Reading Kotlin metadata to type function objects** (#3's first option). This recovers at runtime a type the compiler already knew at the call site. It adds a dependency, and it can't type a generic function class such as `Passthrough<X>()`. A view types it at compile time.
- **Compiler plugin.** A plugin could type raw Flink calls without a view. It would tie Flinkt to Kotlin compiler internals: K2's plugin extension points aren't stable, a plugin has to be released for each compiler version, and IDEs need it too. It could become an optional layer later, for example to flag raw Flink calls that produce Flinkt-modeled types.
- **`ResultTypeQueryable` functions.** Flink's raw operators honor a function that reports its own output type (executed). That makes it a useful complement for code that has to stay on raw Flink. It can't be the main mechanism, because every function has to opt in and lambdas can't.

## Layers and modules

Each module exists because it has a different dependency or release profile:

| Module | Depends on | Why it's separate |
|---|---|---|
| `flinkt-core` | Kotlin stdlib | `@FlinkType`, the schema and type model, the generated-codec SPI (`GeneratedCodec`, `GeneratedTypeModule`), and resolution from `KType` to the type model. Model modules and generated code depend on it, and it doesn't change when Flink does. |
| `flinkt-ksp` | KSP API | Build time only. It must never reach a runtime classpath. |
| `flinkt-view-codegen` | KSP API | A KSP processor that runs only in Flinkt's own build. It generates each adapter's [forwarders](#generated-forwarders) from that line's Flink classes. It isn't published, and users never run it. |
| `flinkt-flink23`, `flinkt-flink22`, `flinkt-flink120` | `flinkt-core`; Flink as `compileOnly` | One per Flink minor line, each owning its views, `typeInfo<T>()`, `TypeInformation`/`TypeSerializer`/`TypeSerializerSnapshot` for generated types, state helpers, and the runtime version guard. |

The adapters are built from one shared source set, compiled against each line, plus small per-line source sets for real differences ([Differences between target lines](flink-compatibility.md#differences-between-target-lines)). A test kit becomes a published module only when something outside this repository needs it. Table integration gets its own per-line modules when it's built.

## Type flow

```text
stream.map { User(it.id, it.name) }
        │   Kotlin compiler: the lambda converts to MapFunction<Event, User>; R = User
        ▼
typeInfo<User>()             inlined at the call site: typeOf<User>() gives classifier, arguments, nullability
        │                    (no kotlin-reflect needed)
        ▼
Flinkt type model            Flink built-in │ collection │ generated (registry) │ otherwise: explicit error
        │
        ▼
TypeInformation<User>        adapter class over the generated codec
        │
        ▼
DataStream.map(fn, typeInfo) Flink's public overload, called on the object the view holds
        │
        ▼
Flink serializer             created by Flink from the TypeInformation and its SerializerConfig
```

`process(SessionFunction())` follows the same path. `SessionFunction : KeyedProcessFunction<Long, UserEvent, Session>` fixes `R = Session` at the call site.

## Views

These rules keep the [user-facing promises](../README.md#flinkt-streams) true:

1. **A view never constructs a Flink stream object.** It holds the object it was given or the one a Flink call returned. Nothing else in Flinkt holds Flink's per-object state, so there's nothing to copy or keep in sync. `.flinkt()` reads nothing from the stream, not even its type: `Transformation.getOutputType()` marks the type as used, and a later `returns()` on the original would then throw.
2. **A view method makes one call to the Flink method of the same name on the held object, and wraps the object that call returns.** Flink's validation, partitioning and operator selection apply unchanged. A fluent method wraps what Flink returned instead of assuming Flink returned `this`.
3. **A method that introduces an element type is `inline` with a reified type parameter, and it takes Flink's own function type.** A Kotlin lambda converts to that type, and a function object passes through unchanged, so both get their result type from the static type at the call site. Each such method has a non-inline overload that takes the `TypeInformation` explicitly, for generic code where the type isn't known.
4. **The adapter calls only `@Public` and `@PublicEvolving` Flink methods** ([dependency policy](flink-compatibility.md#dependency-policy-for-flink-apis)). Where Flink's explicit-type overload is `@Internal`, as `process(fn, TypeInformation)` is on every target line, the view calls the one-argument method and then `returns(TypeInformation)`. Flink's `process(fn)` runs its `TypeExtractor` with missing types allowed, and `returns()` replaces the result before anything reads it.

A view offers two kinds of Flink methods:

- **Type-introducing operators**, such as `map`, `keyBy` and keyed `process`, are written by hand, because each one needs rule 3.
- **Every other method** a view offers only forwards. These methods return the same element type, a sink, or a value: fluent configuration, `filter`, `union`, repartitioning, `sinkTo`, `print`, `executeAndCollect`. They are generated.

A method missing from a view is a compile error. Views can therefore grow over releases without silently changing any existing call, and until then the method is reached through `asFlink()`.

### Generated forwarders

Each view names the Flink class it holds and the Flink methods it forwards:

```kotlin
@FlinkView(
    DataStream::class,
    forward = ["filter", "union", "rebalance", "sinkTo", "print", "executeAndCollect", ...],
)
public open class FlinktDataStream<T> @PublishedApi internal constructor(
    @PublishedApi internal open val flink: DataStream<T>,
) : FlinktDataStreamForwarders<T> {
    override fun asFlink(): DataStream<T> = flink

    public inline fun <reified R> map(fn: MapFunction<T, R>): FlinktSingleOutputStreamOperator<R> = ...
}
```

When Flinkt builds an adapter, `flinkt-view-codegen` reads the listed methods from that line's Flink classes on the compile classpath and generates a `sealed interface FlinktDataStreamForwarders<T>`. The view implements it, so the forwarded methods are members, and users need no imports. Each generated method follows rule 2: it calls the Flink method on `asFlink()` and wraps a returned stream in its view. A parameter that takes a Flink stream with a view takes the view, including varargs, so `union` accepts views. A Java collection comes back as the read-only Kotlin type.

The generator rejects anything that would break a view rule, and the build fails with the reason:

| The listed method | Result |
|---|---|
| doesn't exist on the line being built | build error naming the method and the Flink version |
| returns a type built from a method type parameter (`map`, `process`, `connect`, `getSideOutput`) | build error: write it by hand with a reified type parameter |
| returns a Flink stream or builder type that has no view (`BroadcastStream`, `WindowedStream`) | build error: forwarding it would let a chain leave the view without `asFlink()` |
| is `@Internal` (for example `getTransformation`) | build error |
| is `@Experimental` (for example 2.x `enableAsyncState`) | build error until the [dependency policy](flink-compatibility.md#dependency-policy-for-flink-apis) documents an exception |
| has an overload that is deprecated, or whose signature uses a deprecated type | that overload is skipped and recorded in the generator's report |

A name is listed once and covers all of its overloads, so each overload has to pass. `broadcast()` returns a stream, but `broadcast(MapStateDescriptor...)` returns `BroadcastStream`, so `broadcast` can't be listed until the generator can select a single overload.

The generator runs against every line's classes. A listed method is therefore checked on every line, and a method that exists on only some lines fails the build on the others. Line-specific methods go in the line's own source set.

Two limitations came out of the spike:

- **Parameter names.** Flink's jars carry no parameter names (they read as `p0`, `p1`), so the generator derives them from the method or parameter type: `name`, `parallelism`, `filterFunction`, `dataStreams`. Kotlin allows named arguments, which makes these names part of the source API. Each adapter's checked-in ABI dump records them, so a change shows up in review.
- **The API follows Flink's jar.** A method signature that Flink adds to a listed name reaches Flinkt's public API without anyone choosing it. Before the deprecated-type rule, the 1.20 adapter exposed two extra `sinkTo` overloads that take the legacy `connector.sink.Sink`. The checked-in ABI dump and a cross-adapter API comparison catch that kind of drift ([testing.md](testing.md#generated-forwarders)).

The generator checks the stability annotations of the methods it forwards. Flink 1.20's annotations have class retention, and KSP reads them from bytecode, so the check works on every line. The Flink methods that hand-written view code calls are covered by a [separate test](testing.md#flink-adapter-contract). Evidence: the [view-codegen spike](https://github.com/Lychee-Technology/flinkt/tree/dd2d0f18f6d683107c5ff289c2e6d8bf93d17029/spikes/architecture-reset/view-codegen) generated the same 33 members from 2.3.0 and 1.20.5, compiled them in explicit-API mode, ran a job through them, and failed each negative case with its reason on both lines.

### No environment view

There is no view over `StreamExecutionEnvironment`. The only thing an environment view could add is typed sources, and Flink's typed `fromSource(…, TypeInformation)` is `@Experimental` on every target line. A source reports its own type through `ResultTypeQueryable`, and `fromData` takes a `TypeInformation`, so `typeInfo<T>()` goes there, and the stream enters with `.flinkt()`.

## Generated code and the adapter

KSP generates, for each `@FlinkType` declaration:

- a codec implementing `GeneratedCodec<T>`, which reads fields and calls the constructor directly, and reads and writes through `java.io.DataInput`/`DataOutput`;
- schema metadata;
- one `GeneratedTypeModule` per compilation, registered in `META-INF/services` and discovered with `java.util.ServiceLoader`.

Generated code mentions no Flink type, so it doesn't change with the Flink line. Flink's `DataInputView` and `DataOutputView` implement `DataInput` and `DataOutput`, so the adapter passes them through without copying.

Generated code never subclasses a Flink class. The Flink protocol classes differ between lines: in 1.20, `TypeInformation.createSerializer(ExecutionConfig)` is the abstract factory and `TypeSerializerSnapshot.resolveSchemaCompatibility(snapshot)` has a default. In 2.x, `createSerializer(SerializerConfig)` is the abstract factory and `resolveSchemaCompatibility` is abstract. A generated `TypeInformation` would therefore need one build per line. Instead, each adapter has one `TypeInformation`, one `TypeSerializer` and one `TypeSerializerSnapshot` implementation shared by every generated type. That keeps comparators and snapshot logic in one place. Flink creates serializers from `TypeInformation` with its own configuration, and Flinkt doesn't keep global serializer instances.

A consuming module reuses generated metadata from its dependencies and doesn't regenerate code for classes it doesn't own.

## State helpers

Two helper families have different names because they return different things:

- `valueStateDescriptor<T>(name)`, `listStateDescriptor<T>(name)` and `mapStateDescriptor<K, V>(name)` build Flink descriptors. Users need descriptors to configure TTL and to pass to Flink APIs that take one, such as broadcast state.
- `runtimeContext.valueState<T>(name)`, `listState<T>(name)` and `mapState<K, V>(name)` bind state through `getState`, `getListState` and `getMapState` with such a descriptor, and return Flink's `ValueState`, `ListState` or `MapState`.

Binding stays a visible call in `open()`. A property delegate could hide it, but then the point where state becomes available would be less obvious. Flink 2.x's asynchronous state API (`org.apache.flink.api.common.state.v2`, `@Experimental`) isn't covered yet.

## Open questions

These are decided with the serializer implementation, because each one fixes a persisted format:

- **Stable identity.** The logical-ID policy for sealed subtypes and enum constants.
- **Binary layout.** The null representation, such as a null bitmap, and the encoding of strings and other nested values.
- **Value classes.** When a value class may use its underlying type's serializer.
- **Schema evolution.** The evolution matrix, meaning which changes are compatible as-is, after migration, or not at all ([testing.md](testing.md#serializer-snapshot-compatibility)).

These are sequencing questions that don't affect the architecture:

- **Views after the first slice.** Which Flink types get views next: windows, `connect`, broadcast, joins.
- **Typed sources.** Whether typed-source helpers are worth adding, given that Flink's typed `fromSource` is `@Experimental`.
- **Generator refinements.** Selecting a single overload (needed for `broadcast()`), a documented opt-in for `@Experimental` methods, and whether to read real parameter names from Flink's source jars.
