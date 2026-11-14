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
- **Explicit persisted-state compatibility.** Every restore result is backed by old-bytes and savepoint evidence. Values evolve as Flink POJOs do, and keys don't evolve structurally ([Schema evolution](#schema-evolution)).
- **Keys that stay found.** A modeled type is accepted as a key only where Flinkt can check mechanically that its hash code, equality and bytes are the same in every JVM that restores its state.

Two things that looked like requirements are preferences, and this design gives them up. One is that a Flinkt stream is directly assignable to `DataStream<T>`. The other is that leaving Flinkt needs no syntax at all. Both would come at the expense of the requirements above, as the next section shows.

## Decision: streams are views, not Flink subtypes

A Flinkt stream is a view that holds exactly one Flink stream object. `FlinktDataStream<T>` holds a `DataStream<T>`, `FlinktSingleOutputStreamOperator<T>` holds a `SingleOutputStreamOperator<T>`, and `FlinktKeyedStream<T, K>` holds a `KeyedStream<T, K>`. A view doesn't extend the Flink class. The README states [the rules users rely on](../README.md#flinkt-streams). The [implementation rules](#views) below keep them true.

The first version of this design, in PR #1, made each Flinkt stream a subclass of the Flink class it stood for (`FlinktDataStream<T> : DataStream<T>`). Five reviews then found a string of problems, and each fix added an exception to the model: existing keyed streams rejected, adapted operators losing their configuration methods, `union` as a forced exit, a per-line list of exits, and function-object operators without a result type ([#3](https://github.com/Lychee-Technology/flinkt/issues/3)). The [architecture review](https://github.com/Lychee-Technology/flinkt/pull/1) traced them to two questions the subtype design couldn't answer well:

- **Which object owns a stream's Flink state?** A subtype can't *be* the object Flink created, so it has to build a second one, on entry and around every object a Flink call returns. Flink keeps some state on stream objects rather than on their transformations: a keyed stream's partitioning, `forceNonParallel()`, requested side outputs, async state in 2.x, and a source's parallelism. A second object doesn't have that state.
- **Which calls are Flinkt's?** A subtype inherits every Flink method under the same name. Whether a call gets Kotlin types then depends on details the user can't see at the call site: lambda versus function object, `final` versus overridable, whether an override exists, and whether an override could be reified at all.

The following were executed against Flink 1.20.5, 2.2.1 and 2.3.0 with Kotlin 2.4.20 ([spike](https://github.com/Lychee-Technology/flinkt/tree/4cccdc5ec6438d2f76e6598b0373fd2b1f563471/spikes/architecture-reset)):

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
| `flinkt-core` | Kotlin stdlib | `@FlinkType`, the schema and type model, the generated-codec SPI (`GeneratedCodec`, `GeneratedTypeModule`), and resolution from `KType` to the type model. It also holds the persisted-schema model, the schema-compatibility planner and the schema-driven reader that [migration](schema-evolution.md#restore-model) uses. Model modules and generated code depend on it, and it doesn't change when Flink does. |
| `flinkt-ksp` | KSP API | Build time only. It must never reach a runtime classpath. |
| `flinkt-view-codegen` | KSP API; ASM for the extraction script | A KSP processor that runs only in Flinkt's own build. It generates each adapter's [forwarders](#generated-forwarders) from that line's Flink classes, with the parameter names that a script in the same module reads from those class files. It isn't published, and users never run it. |
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

The hand-written part, entering with `.flinkt()`, `asFlink()` and the type-introducing `map`, `keyBy` and keyed `process`, is implemented in [#9](https://github.com/Lychee-Technology/flinkt/issues/9). [#10](https://github.com/Lychee-Technology/flinkt/issues/10) chose the forwarded methods ([The first slice's forward lists](#the-first-slices-forward-lists)).

These rules keep the [user-facing promises](../README.md#flinkt-streams) true:

1. **A view never constructs a Flink stream object.** It holds the object it was given or the one a Flink call returned. Nothing else in Flinkt holds Flink's per-object state, so there's nothing to copy or keep in sync. Wrapping an object reads nothing from it, not even its type, whether the object entered with `.flinkt()` or a Flink call returned it. `Transformation.getOutputType()` marks the type as used, and a later `returns()` on the Flink object would then throw.
2. **A view method makes one call to the Flink method of the same name on the held object, and wraps the object that call returns.** Flink's validation, partitioning and operator selection apply unchanged. A fluent method wraps what Flink returned instead of assuming Flink returned `this`. The one second call is rule 4's `returns()`, which completes the one Flink operation the view method stands for. No other extra call is allowed.
3. **A method that introduces an element type is `inline` with a reified type parameter, and it takes Flink's own function type.** A Kotlin lambda converts to that type, and a function object passes through unchanged, so both get their result type from the static type at the call site. Each such method has a non-inline overload that takes the `TypeInformation` explicitly, for generic code where the type isn't known. The inline overload calls that one with `typeInfo<R>()`, so the inline body, which a user's compiler copies into the user's class files, makes no Flink call of its own, and the object a view holds stays private. Both overloads take Flink's parameter names, as forwarders do: `mapper`, `key` and `keyType`, `keyedProcessFunction`, and `outputType`.
4. **Hand-written adapter code calls only `@Public` and `@PublicEvolving` Flink methods**, apart from `@Experimental` exceptions that the [dependency policy](flink-compatibility.md#dependency-policy-for-flink-apis) lists. Where Flink's explicit-type overload is `@Internal`, as `process(fn, TypeInformation)` is on every target line, the view calls the one-argument method and then `returns(TypeInformation)`. Flink's `process(fn)` runs its `TypeExtractor` with missing types allowed, and `returns()` replaces the result before anything reads it. Generated forwarders carry the status Flink declares instead: they refuse `@Internal` and put `@Experimental` behind an opt-in ([Generated forwarders](#generated-forwarders)).

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
public open class FlinktDataStream<T> internal constructor(
    private val stream: DataStream<T>,
) : FlinktDataStreamForwarders<T> {
    override fun asFlink(): DataStream<T> = stream

    public inline fun <reified R> map(mapper: MapFunction<T, R>): FlinktSingleOutputStreamOperator<R> =
        map(mapper, typeInfo<R>())

    public fun <R> map(mapper: MapFunction<T, R>, outputType: TypeInformation<R>): FlinktSingleOutputStreamOperator<R> =
        FlinktSingleOutputStreamOperator(stream.map(mapper, outputType))
}
```

When Flinkt builds an adapter, `flinkt-view-codegen` reads the listed methods from that line's Flink classes on the compile classpath and generates a `sealed interface FlinktDataStreamForwarders<T>`. The view implements it, so the forwarded methods are members, and users need no imports. Each generated method follows rule 2: it calls the Flink method on `asFlink()` and wraps a returned stream in its view. A parameter that takes a Flink stream with a view takes the view, including varargs, so `union` accepts views. A Java collection comes back as the read-only Kotlin type, and a parameter that takes one accepts a read-only collection. A parameter or result that Flink annotates `@Nullable` is nullable, and the others aren't.

The generator follows one principle: a forwarder carries Flink's API as Flink declares it. That covers overloads, parameter names, and stability status. The generator only refuses what would break a view rule. Where Flinkt is stricter than Flink, the reason is type propagation, not taste.

| The listed method | Result |
|---|---|
| doesn't exist on the line being built | build error naming the method and the Flink version |
| returns a type built from a method type parameter (`map`, `process`, `connect`, `getSideOutput`) | build error: write it by hand with a reified type parameter |
| returns a Flink stream or builder type that has no view (`BroadcastStream`, `WindowedStream`) | build error: forwarding it would let a chain leave the view without `asFlink()` |
| is `@Internal` (for example `getTransformation`) | build error: `@Internal` isn't public API in Java either |
| returns a `TypeInformation` (`getType`) | build error: its forwarder would read the type of the stream the view holds, which [rule 1](#views) forbids |
| is deprecated | forwarded and annotated with Kotlin's `@Deprecated`, so a Kotlin caller gets the warning a Java caller gets |
| uses a deprecated type in its signature, but isn't deprecated itself (1.20's `sinkTo` for the legacy `Sink`) | forwarded unannotated, as Flink declares it |
| is `@Experimental` (for example 2.x `enableAsyncState`, which no view lists) | forwarded and annotated with `@ExperimentalFlinkApi`, a Kotlin `@RequiresOptIn` marker at warning level, so a call without `@OptIn` warns |

A Flink stream or builder type is a `DataStream`, or any class of Flink's stream API package, `org.apache.flink.streaming.api.datastream`, nested classes included. `DataStreamSink` is the exception: a sink ends a chain, its methods only configure it, and `print` and `sinkTo` return it as it is. The rule goes by package rather than by a list, so a builder type that a new Flink line adds there fails the build until it has a view or is listed as an exception, instead of being forwarded unnoticed.

The opt-in marker implements the [dependency policy](flink-compatibility.md#experimental): functionality built on an experimental Flink API is itself explicitly experimental.

A name is listed once and covers all of its overloads, so each overload has to pass. `broadcast()` returns a stream, but `broadcast(MapStateDescriptor...)` returns `BroadcastStream`, so `broadcast` can't be listed until the generator can select a single overload.

The views follow Flink's class hierarchy. When a superclass of a view's Flink class has a view, as `DataStream` has for `SingleOutputStreamOperator` and `KeyedStream`, the generated interface extends that view's, and `asFlink()` narrows to the subclass. A method is forwarded by the view of the class that declares it, and every view below gets it: `filter` is listed on `FlinktDataStream`, and `FlinktKeyedStream` has it too. Listing it on a view below fails the build and names the view to list it on. A method that a Flink subclass overrides is the subclass's own, so its view can forward it again, and the forwarder overrides the one above with the subclass's view as the result, as `DataStreamSource.setParallelism` returns `DataStreamSource`. A name can have overloads on both levels: on 2.x, `KeyedStream` overrides `flatMap(FlatMapFunction, TypeInformation)` and inherits `DataStream`'s `flatMap(FlatMapFunction)`. Then each view forwards the overloads its class declares, so the name is listed on both. Listing it only on the view below fails the build and names the view above, so no overload of a listed name goes unforwarded and unchecked.

The generator runs against every line's classes. A listed method is therefore checked on every line, and a method that exists on only some lines fails the build on the others. Line-specific methods go in the line's own source set. Deprecated overloads that only one line has are Flink's own difference between lines. They appear as listed per-line differences in the cross-adapter API comparison. In the spike, 1.20 had six such overloads.

#### The first slice's forward lists

[#10](https://github.com/Lychee-Technology/flinkt/issues/10) chose these, within the generator's rules:

| View | Forwards |
|---|---|
| `FlinktDataStream` | `filter`, `union`, `rebalance`, `rescale`, `shuffle`, `forward`, `global`, `partitionCustom`, `assignTimestampsAndWatermarks`, `sinkTo`, `print`, `printToErr`, `executeAndCollect` |
| `FlinktSingleOutputStreamOperator` | `name`, `uid`, `setUidHash`, `setDescription`, `setParallelism`, `setMaxParallelism`, `forceNonParallel`, `setBufferTimeout`, `disableChaining`, `startNewChain`, `slotSharingGroup` |
| `FlinktKeyedStream` | nothing of its own: `KeyedStream` declares none of these methods, and the view offers `DataStream`'s through `FlinktDataStream`'s forwarders |

Fluent configuration that returns the view is what keeps `.map { … }.name("users").keyBy { … }.process(…)` in Flinkt. On 1.20 the lists also forward six overloads that 2.x removed: `partitionCustom` by field position and by field expression, and `assignTimestampsAndWatermarks` with the two old assigner types, all deprecated, and the two `sinkTo` overloads for the legacy `connector.sink.Sink`. They're the listed per-line differences of the cross-adapter comparison.

These are left out, and reached through `asFlink()`:

- **`enableAsyncState()`**, on `SingleOutputStreamOperator` and `KeyedStream` in 2.x. It's `@Experimental`, Flink 2.x's async state (API v2) is out of scope for [#31](https://github.com/Lychee-Technology/flinkt/issues/31), and a forwarder would make it part of Flinkt's public API. Async state enabled on the Flink object before `.flinkt()` is kept: an operator's flag is on its transformation, and a keyed stream's on the object the view holds ([State on stream objects](flink-compatibility.md#state-on-stream-objects)).
- **`returns`**. A view's operators give Flink the type from the call site, and `returns` would retype the stream afterwards. Two of its overloads, `returns(Class)` and `returns(TypeHint)`, take the type from Flink's `TypeExtractor`, which Flinkt doesn't build on ([Do not build correctness around `TypeExtractor`](flink-compatibility.md#do-not-build-correctness-around-typeextractor)).
- **`broadcast`**, because `broadcast(MapStateDescriptor...)` returns `BroadcastStream`, which has no view. It waits for overload selection ([Open questions](#open-questions)).
- **The legacy sinks**: `addSink`, whose `SinkFunction` moved to a `legacy` package in 2.x, and `writeToSocket` and the deprecated `writeUsingOutputFormat`. `sinkTo` is the sink API on every line.
- **`collectAsync`**, which is `@Experimental`, and `addMetricVariable`, which only 2.3 has.
- **The getters**, such as `getParallelism`, `getName` and `getExecutionConfig`. They configure nothing, and a chain doesn't continue from what they return. On `asFlink()` Kotlin reads them as properties, as in `asFlink().parallelism`, which a forwarder wouldn't offer.
- **The keyed aggregations**, `reduce`, `sum`, `min`, `max`, `minBy` and `maxBy`. The generator would forward them, but they aren't part of the first slice, and all but `reduce` select a field by position or expression, which depends on how the `TypeInformation` for generated types ([#12](https://github.com/Lychee-Technology/flinkt/issues/12)) exposes fields.
- **Type-introducing and builder methods**, which the generator refuses: `getSideOutput`, `cache`, windows, `connect`, joins and the like ([Views after the first slice](#open-questions)).

#### Parameter names

Flink's jars have no `MethodParameters` attribute, so the compiler and KSP see `p0`, `p1`. Kotlin allows named arguments, which makes parameter names part of the source API. The names are still in the jars: Flink compiles with debug information, and each method's LocalVariableTable names its parameters. The forwarders use Flink's own names:

- **Extraction.** A script in Flinkt's build reads each line's class files with ASM. It takes each public method's JVM descriptor and its parameter names from the LocalVariableTable, and writes one row per method to a per-adapter `flink-api.tsv`. It prefers a `MethodParameters` attribute if Flink ever adds one. The file is checked in and records the Flink version it came from. The script reads the same jars the adapter compiles against, so each descriptor is the class file's own. It reads the classes the views hold and their supertypes, and leaves out abstract methods, which have no LocalVariableTable and which a view of a concrete class never calls. It fails if a public method has no parameter names, which would mean Flink stopped compiling with debug information. The adapter's `extractFlinkApi` task runs it, and `checkFlinkApi`, part of `check`, fails when the checked-in file isn't what it reads from the jars, so the file can't drift from them unreviewed.
- **Lookup.** KSP receives the file as a tracked Gradle input, so editing or regenerating it reruns generation. It looks up each forwarded method by its declaring class and descriptor. A missing entry, or a file extracted from a different Flink version than the one dependency resolution put on the compile classpath, fails the build. KSP's `Resolver.mapToJvmSignature` omits the array marker of a Java varargs parameter, so the generator corrects that before the lookup.
- **Review.** The names file is where name changes are reviewed. The adapter's API dump records JVM signatures, which don't include parameter names.

The result is `executeAndCollect(jobExecutionName, limit)`, `print(sinkIdentifier)`, `reduce(reducer)`, `setBufferTimeout(timeoutMillis)` and `union(vararg streams)`. Adopting a Flink line includes extracting its file, and the file's diff shows every name that changed.

The spike also extracted the names from Flink's `-sources.jar` with JavaParser. After correcting two JavaParser descriptor errors (the varargs array marker, and nested types written as `Outer/Inner`), it produced the same rows as the class files on both lines. Reading the class files avoids the sources jar, the parser, and its descriptor errors.

#### Documentation

Generated forwarders have no KDoc in the first version. Whether to add it later, and how, is an [open question](#open-questions).

#### Drift

A signature that Flink adds to a listed name still reaches Flinkt's public API without anyone choosing it. The checked-in API dump of each adapter, and the cross-adapter comparison, make that visible in review ([testing.md](testing.md#generated-forwarders)).

A change in a listed method's stability status reaches Flinkt's API the same way. A method that becomes `@Experimental` or deprecated is still forwarded, now with the opt-in marker or `@Deprecated`. Its JVM signature doesn't change, so the API dump doesn't show it. The cross-adapter comparison does, because it compares the annotations the generator adds and fails until a status that differs between lines is listed. Only a change to `@Internal` fails generation.

The generator checks the stability annotations of the methods it forwards. Flink 1.20's annotations have class retention, and KSP reads them from bytecode, so the check works on every line. The Flink methods that hand-written view code calls are covered by a [separate test](testing.md#flink-adapter-contract).

Evidence: the [view-codegen spike](https://github.com/Lychee-Technology/flinkt/tree/4cccdc5ec6438d2f76e6598b0373fd2b1f563471/spikes/architecture-reset/view-codegen) ran against 2.3.0 and 1.20.5:

- it read 103 and 120 methods' parameter names from the class files;
- it generated the forwarders with those names and compiled them in explicit-API mode;
- it ran a job through them;
- it refused each case above with its reason;
- Kotlin callers got Flink's deprecation and experimental warnings;
- the names-file checks behaved as described.

### No environment view

There is no view over `StreamExecutionEnvironment`. The only thing an environment view could add is typed sources, and Flink's typed `fromSource(…, TypeInformation)` is `@Experimental` on every target line. A source reports its own type through `ResultTypeQueryable`, and `fromData` takes a `TypeInformation`, so `typeInfo<T>()` goes there, and the stream enters with `.flinkt()`.

## Generated code and the adapter

KSP generates, for each `@FlinkType` declaration:

- a codec implementing `GeneratedCodec<T>`, which reads fields and calls the constructor directly, and reads and writes through `java.io.DataInput`/`DataOutput`;
- the type's persisted schema ([#5](persisted-format.md#snapshot));
- a migration constructor, which builds the current value from the values a migration resolved and calls the primary constructor directly ([Constructing current values](schema-evolution.md#constructing-current-values));
- one `GeneratedTypeModule` per compilation, registered in `META-INF/services` and discovered with `java.util.ServiceLoader`.

Generated code mentions no Flink type, so it doesn't change with the Flink line. Flink's `DataInputView` and `DataOutputView` implement `DataInput` and `DataOutput`, so the adapter passes them through without copying.

The view a codec writes to isn't part of its format: the bytes must be the same on every view. Flink's views don't all write the same bytes for the same call (checked on 1.20.5, 2.2.1 and 2.3.0):

- `writeFloat` and `writeDouble` write one canonical NaN on `DataOutputSerializer` and `DataOutputViewStreamWrapper`, as the `java.io.DataOutput` contract says. On `AbstractPagedOutputView`, which Flink's sort buffers write records to, they keep each NaN's raw bits.
- `writeBytes(String)` advances a `DataOutputSerializer` by twice the string's length, so it emits the string's bytes and then as many stale buffer bytes.

So a Flinkt-owned format leaves no byte to the view's implementation. A codec converts floating-point values itself ([Deterministic bytes](#deterministic-bytes)), and #5's string encoding uses only calls that every view writes alike ([Records](persisted-format.md#records)).

Generated code never subclasses a Flink class. The Flink protocol classes differ between lines: in 1.20, `TypeInformation.createSerializer(ExecutionConfig)` is the abstract factory and `TypeSerializerSnapshot.resolveSchemaCompatibility(snapshot)` has a default. In 2.x, `createSerializer(SerializerConfig)` is the abstract factory and `resolveSchemaCompatibility` is abstract. A generated `TypeInformation` would therefore need one build per line. Instead, each adapter has one `TypeInformation`, one `TypeSerializer` and one `TypeSerializerSnapshot` implementation shared by every generated type. That keeps snapshot logic in one place. Flinkt supplies no comparators ([Where the rules apply](#where-the-rules-apply)). Flink writes the snapshot class's fully-qualified name into every checkpoint and instantiates it by name on restore, so that class has the same name on every adapter line, `io.flinkt.flink.FlinktSerializerSnapshot`, and no per-line subclass or wrapper becomes the class Flink records ([The snapshot class](persisted-format.md#the-snapshot-class)). Flink creates serializers from `TypeInformation` with its own configuration, and Flinkt doesn't keep global serializer instances.

A consuming module reuses generated metadata from its dependencies and doesn't regenerate code for classes it doesn't own.

### Rebuilding through the constructor

Decided in [#42](https://github.com/Lychee-Technology/flinkt/issues/42).

`deserialize`, `copy` and the migration constructor all build a record by calling its primary constructor with the constructor properties they read. They return the value that was written only if constructing it changes none of them. #11 allows `var` constructor properties and `init` blocks, and an `init` block can break that:

```kotlin
@FlinkType
data class Counter(var count: Int) { init { count++ } }
```

`Counter(0)` holds 1 and is written as 1. Decoding calls `Counter(1)`, which holds 2, and copying that gives 3. Each restore of a `ValueState<Counter>` adds one, and so does each `value()` on RocksDB, which decodes on every read. So does each hop between chained operators: with object reuse off, Flink's default, Flink copies every record it passes from one chained operator to the next (`CopyingChainingOutput`, checked in 1.20.5 and 2.3.0). The schema is unchanged and nothing fails.

| In a `@FlinkType` data class | First release |
|---|---|
| `init` block that only checks its arguments, such as `require(count >= 0)` | supported |
| `init` block, or code it calls, that changes a constructor property of its own class or of a value it was passed | not supported, and not checked: a documented contract |
| a value that's written or copied while it fails those checks, such as one whose `var` was assigned after construction | not supported, and not checked: a documented contract |

**The contract.** Constructing a record must leave each constructor property equal to the argument passed for it, and must not change the arguments themselves. An `init` block may check its arguments and throw, but must not change a constructor property or mutate a component. Flinkt doesn't check this. A class that breaks it compiles, and its values change on each read and copy.

That rules out normalization too. `data class Email(var address: String) { init { address = address.lowercase() } }` survives a round trip of `Email("A")`, which already holds `"a"`. But after `email.address = "B"` it's written as `"B"` and decodes as `"b"`. A `var` can hold any value when it's written, not only one the constructor produced, so a constructor that's idempotent on its own output isn't enough.

The `var` and the `init` block needn't be in the same class. `data class Outer(val inner: Inner) { init { inner.count++ } }` over `data class Inner(var count: Int)` is written with `count == 1`, decodes as 2 and copies as 3, though `Outer` has only `val`s and `Inner` has no `init` block. A `val` constructor property can't be reassigned, so a record whose data classes have only `val` constructor properties, at every depth, can't break the contract unless code casts a decoded `List` or `Map` to its mutable class and changes it. The key rules ([Map keys](#map-keys), [Eligibility](#eligibility)) require `val`s and also reject a collection anywhere in a key, so that cast has nothing to reach in one.

**Values must pass the checks when they're rebuilt.** A rebuild calls the constructor with the properties a value holds when it's written or copied, not with the arguments it was first built from. So every value that's written or copied must pass the checks of its own `init` block and its components', also after a `var` was assigned. In `data class Checked(var count: Int) { init { require(count >= 0) } }`, `Checked(1).apply { count = -1 }` changes nothing at construction, yet can't be rebuilt. `serialize` doesn't call the constructor, so it writes the value without error. `deserialize` and `copy` do, and they fail with the `IllegalArgumentException` that `require` throws. So does whatever in Flink decodes or copies the value, such as a copy between chained operators, a `value()` on RocksDB or a restore on the heap backend. A checkpoint can hold such a value, and every read of it from that checkpoint fails the same way, because its bytes don't change. Flinkt doesn't check this when it writes either. KSP can't see the checks, and running the constructor on every `serialize` to find out would add a constructor call per record to every write. (`ConstructorRebuildTest` in `flinkt-core` rebuilds `Counter`, `Email`, `Outer` and `Checked` with codecs that read the constructor properties and call the primary constructor, as generated ones will, and compiles them with Kotlin 2.4.0 and 2.4.21.)

Why a contract rather than a compile error:

- **KSP can't see an `init` block.** KSP 2.3.13's symbol API has no node for one, and a class's declarations look the same with or without it. For `Counter`, for the same class without its `init` block, and for one whose block only calls `require`, KSP lists the same property, the same synthetic `component1` and `copy`, and the same `<init>`, which is the primary constructor (checked through the compile-test harness's KSP on Kotlin 2.4.0 and 2.4.21). So a rule that rejects a `var` constructor property in a class with an `init` block can't be written. It wouldn't be enough anyway. KSP shows no function body, so the rule would also reject a block that only calls `require`, and it would miss `Outer`, whose `init` block and `var` are in different classes. Only a Kotlin compiler plugin sees `init` blocks, and its API isn't stable across compiler releases.
- **Rejecting every `var` constructor property** wouldn't make the contract hold on its own. It stops an `init` block from reassigning a property, but not from casting a decoded `List` or `Map` to its mutable class and changing it, so it would also take rejecting collections, as the key rules do. And it removes a form #11 supports for values, where a state value updated in place needs `var`s, to prevent a mistake most classes with a `var` don't make. A key loses little by the `val` rule: changing a key after it's stored is already a bug, so a `val` serves wherever a `var` would.
- **A runtime check in the codec**, comparing each property with its argument after construction, would add a comparison per property, and an `equals()` call per nested record, to every decode and copy. It would still miss `Outer`, whose property is the very object it was passed, with only its contents changed.

Because nothing checks the contract, the generated codecs come with a check that users can run in their own tests. It compares the bytes of `serialize(v)` with those of `serialize(deserialize(serialize(v)))` and of `serialize(copy(v))`. Comparing bytes rather than calling `equals()` catches a change at any depth, `Outer`'s included, and doesn't depend on a hand-written `equals()`. It checks only the values it's given: `Email` passes for a freshly built value and fails only for one whose `address` was assigned afterwards. For `Checked(1)` after `count = -1`, it fails with the `init` block's exception, as a read would. Which artifact publishes it is still open ([Testkit](testing.md#testkit)).

Turning the contract into a compile error later, if KSP comes to show `init` blocks, changes no bytes, but it rejects classes that compiled before.

## Type model and registry

Implemented in [#6](https://github.com/Lychee-Technology/flinkt/issues/6), in `flinkt-core`.

A Kotlin type reaches what serializes it in two steps, and `TypeRegistry` keeps them apart:

```text
KType (typeOf<T>())
    │  model(type): structural analysis, for every type Kotlin can express
    ▼
FlinktType               classifier, arguments with variance, nullability at every level, each node's shape
    │  resolve(type): supported resolution
    ▼
Resolution               BuiltIn(kind): a built-in, which the adapter gives Flink's own TypeInformation (#7)
                         Codec(GeneratedCodec): a type whose bytes Flinkt's generated code writes
                         or UnsupportedTypeException naming the canonical type and the reason
```

**The model represents; resolution supports.** Analysis models every planned shape from the start: built-ins, `List` and `Map` and the collection types Flinkt rejects, generated records, value classes, enums, sealed hierarchies, objects, type parameters, and any other class by its classifier only. A shape that a release doesn't support yet is still modeled and fails in resolution, so each later shape changes a resolution branch, not the model. Analysis reads only what `KType` carries without `kotlin-reflect`. A generated declaration's shape comes from the schema its module registered, because `typeOf<T>()` can't tell a sealed class or a value class apart without `kotlin-reflect`.

**Canonical form.** A model's `toString()` is the Kotlin type with qualified names, every argument and every `?`, such as `kotlin.collections.Map<com.example.UserId, kotlin.collections.List<com.example.Event?>>`. Errors use it. `KType` has no type aliases or source spelling, so the form doesn't either. Equal `KType`s give equal models. A platform type from Java is modeled as non-null, and a Java collection, `(Mutable)List<T>!`, as the read-only `List<T>`. `MutableList<T>` and `List<T>` have the same classifier, so the analyzer tells them apart the only way `KType` allows, by equality: it rebuilds the mutable type from the classifier and arguments through `kotlin.jvm.internal.Reflection`, the factory compiled `typeOf` calls use, and compares. With `kotlin-reflect` present that factory builds `kotlin-reflect`'s types, so the comparison doesn't depend on which implementation the application has. #17 tests both configurations ([Collections](#collections)).

**Codec SPI.** `GeneratedCodec<T>` reads and writes through `DataInput` and `DataOutput` and copies. A codec holds no state that changes between calls. A `CodecFactory` creates it when the type is resolved, and gets a `CodecContext` with the codecs of the type's arguments and a way to resolve the codecs of the types its values contain. The context is valid only while the factory runs, so a record's codec fixes its nested codecs at creation and never looks one up per value. A type whose codec would need itself fails until recursive types are supported.

**Built-ins resolve to their kind.** Core holds no byte layout, and that includes the scalars. A built-in type resolves to its `BuiltInKind`, not to a codec, in every registry, whatever modules it has. The adapter gives Flink its own `TypeInformation` for the kind, so a top-level built-in's bytes are Flink's ([Deterministic bytes](#deterministic-bytes)), and a record's generated codec writes a built-in field itself, in #5's format. The SPI has no way for a module to register a built-in's codec, which would let any jar on the classpath define the bytes of `Int` or `String`, and a declaration registered for a built-in class fails the registry. A Flinkt codec for a built-in is needed only where one is composed, as a `List<String>` element (#17) or an `Envelope<String>` argument (#22), and it writes #5's scalar and string format. Those codecs live in `flinkt-core` ([Where the codecs live](persisted-format.md#where-the-codecs-live)), and #17 or #22, whichever lands first, adds them. Until then a codec that asks its context for a built-in's codec fails explicitly.

**Registry.** `TypeRegistry.load(classLoader)` builds a registry from the `GeneratedTypeModule`s that `java.util.ServiceLoader` finds through the class loader the caller supplies. Flink loads each job's code in a class loader of its own and can run several jobs in one JVM, so:

- a generated type is identified by its `Class` object, never by its name. Two classes with the same name in two user-code class loaders are two types, and a lookup in one registry never returns the other's codec;
- each `load` builds a new registry, and nothing is cached in a static field. A static map from class name to codec would turn a cache into JVM-wide type identity, and it would keep a finished job's class loader reachable. The adapter decides how long a registry lives ([#24](https://github.com/Lychee-Technology/flinkt/issues/24) tests the lifecycle).

A type registered twice fails the registry, with an error naming both modules. A model is resolved with the declarations of the registry that resolves it, whichever registry built it: each class's shape is first taken from the resolving registry's own declaration of that class, so the schema that decides support and the factory that creates the codec always come from the same registration. A registry, its schemas and its models never change once built, and their lists are unmodifiable copies, because a registry is shared between threads and a schema is what a snapshot will describe. `@FlinkType` has runtime retention: a consumer's KSP reads it from a dependency jar, and resolution reads it to tell an annotated class whose module wasn't found from a class that isn't annotated.

**Schema model.** A module registers each declaration's schema with its codec factory: a record's fields in primary-constructor order with their names and full types, a value class's underlying property, an enum's constants and a sealed type's subtypes with their `@FlinkId`s, and type parameters. A schema names types by strings, never by `Class`, so a schema read back from a snapshot can describe a type the current classpath doesn't have. A declaration is named by its JVM class name, and a type Kotlin maps to a Java class by its Kotlin name, which is what tells `List` from `MutableList`. A snapshot persists the schema of every declaration its type reaches ([Snapshot](persisted-format.md#snapshot)).

## Type information

Implemented for built-in types in [#7](https://github.com/Lychee-Technology/flinkt/issues/7), in the shared adapter source.

`typeInfo<T>()` is inline and reified. It passes `typeOf<T>()` to a non-inline function, which resolves it with a `TypeRegistry` and gives Flink a `TypeInformation` for the resolution:

- **A built-in kind** gives Flink's own `BasicTypeInfo` for the kind's boxed JVM class, the one `BasicTypeInfo.getInfoFor` returns for that class. Flink then creates its own serializer, such as `IntSerializer` or `StringSerializer`, from its own configuration, so a top-level built-in's bytes are Flink's on every line, and Flinkt adds no persisted format for them. The mapping is a `when` over `BuiltInKind` with no fallback branch, so a kind that core adds doesn't compile in the adapter until it's mapped. Flink has a `BasicTypeInfo` for more classes, such as `BigDecimal`, `Instant` and `Date`, but each mapping fixes a type's state format. So each further one is its own change with its reason recorded, and until then those types fail like any other.
- **Every other type** throws `UnsupportedTypeException` naming the canonical type, core's reason, and the README's [two fixes](../README.md#when-something-isnt-supported): annotate the type with `@FlinkType`, or pass a `TypeInformation` explicitly. That covers nullable types (#15), `List` and `Map` (#17), and every type core can't resolve. A generated type that a module registers fails too, until #12 gives it a `TypeInformation`. It fails before it's resolved, nullable or not, because resolving it would run its module's codec factory and every factory that one calls, only to discard the codec, and a factory that fails first, with a nested type's reason or an exception of its own, would hide why the type fails. Its message gives both fixes too, and says that the annotation is already there and gives it no `TypeInformation` in this release, so that it's the explicit `TypeInformation` that works for now. Nothing falls back to Flink's `TypeExtractor` or to a `GenericTypeInfo` ([flink-compatibility.md](flink-compatibility.md#do-not-build-correctness-around-typeextractor)).

**Registry.** Each call loads a new registry from the generated modules that the current thread's context class loader finds. In a task, Flink's `Task` sets that loader to the job's user-code class loader before it runs user code (checked in the 1.20.5 and 2.3.0 sources). Which loader is current where the job's `main()` builds the graph isn't checked yet. A built-in resolves to its kind in every registry, so for now the choice only decides how a generated type fails. #12 decides which class loader resolves a generated type, with evidence for both places, and #24 how long a registry lives.

The only Flink API that `typeInfo` uses is `TypeInformation` and `BasicTypeInfo`'s constants, both `@Public` on 1.20 and 2.x. The reified function is synthetic in bytecode, so the adapter's ABI dump shows only the non-inline function, whose JVM signature the inlined calls depend on. Compile tests pin the reified function's source API on both compilers.

## State helpers

Two helper families have different names because they return different things:

- `valueStateDescriptor<T>(name)`, `listStateDescriptor<T>(name)` and `mapStateDescriptor<K, V>(name)` build Flink descriptors. Users need descriptors to configure TTL and to pass to Flink APIs that take one, such as broadcast state.
- `runtimeContext.valueState<T>(name)`, `listState<T>(name)` and `mapState<K, V>(name)` bind state through `getState`, `getListState` and `getMapState` with such a descriptor, and return Flink's `ValueState`, `ListState` or `MapState`.

Binding stays a visible call in `open()`. A property delegate could hide it, but then the point where state becomes available would be less obvious. Flink 2.x's asynchronous state API (`org.apache.flink.api.common.state.v2`, `@Experimental`) isn't covered yet.

## Persisted formats

Generated serializers write bytes that users' checkpoints and savepoints keep, and the snapshot Flink stores beside them decides whether a later job may read those bytes. [persisted-format.md](persisted-format.md) is the decision record for both ([#5](https://github.com/Lychee-Technology/flinkt/issues/5)): record format version 1, with fields in name order after a null bitmap, Flink's `StringValue` string format and canonical NaNs, and snapshot format version 1, a binary schema graph in one snapshot class. The value-class representation is still open ([#18](https://github.com/Lychee-Technology/flinkt/issues/18)). The decisions below build on #5: where an example shows a record, a scalar or a nullable value in angle brackets, its bytes are [#5's](persisted-format.md#records).

### Collections

Decided in [#16](https://github.com/Lychee-Technology/flinkt/issues/16).

| Kotlin type | First release |
|---|---|
| `List<T>` | supported |
| `Map<K, V>` | supported |
| `Set<T>` | not supported |
| `Collection<T>`, `Iterable<T>` | not supported |
| `MutableList<T>`, `MutableSet<T>`, `MutableMap<K, V>`, `MutableCollection<T>` | not supported, and not treated as the read-only type |
| arrays, including `IntArray` and the other primitive arrays | not supported |

An unsupported collection type fails explicitly, at compile time as a `@FlinkType` field and in `typeInfo<T>()` as a top-level type. Elements, keys and values can be any modeled type, nullable ones included, so `List<User?>` and `Map<UserId, List<Event?>>` work once their element types do. The one exception is a `Map` key type that might not come back equal after decoding ([Map keys](#map-keys)).

`List` and `Map` cover the README's cases: ordered homogeneous values, key-value structures, nested generics and nullable nested values. `Set` is left out because equal sets have no natural byte order. `setOf(a, b)` equals `setOf(b, a)`, but writing each in iteration order gives different bytes. Fixing that means choosing an order for arbitrary element types and buffering and sorting every set's elements, and no first-release use case needs it. The mutable interfaces are rejected rather than treated as their read-only counterparts. Treating them that way would give two types that Kotlin distinguishes one `TypeInformation`, and it would invite mutating values that Flink may keep in heap state or, with object reuse, pass to chained operators without copying. On the JVM, `MutableList<T>` and `List<T>` are both `java.util.List`. Without `kotlin-reflect` on the runtime classpath, `typeOf` returns a `TypeReference` and distinguishes them only through `KType.equals`. Their `toString()` and classifier are the same. A Java collection that Kotlin sees as a platform type, `(Mutable)List<T!>!`, equals neither and passes a naive mutability check as mutable. With `kotlin-reflect` present, the same call sites return a different `KType` implementation: `toString()` then differs, and a `TypeReference` rebuilt from classifier and arguments equals neither type (both checked on Kotlin 2.4.20). The application decides whether `kotlin-reflect` is present, so a resolver that relies on either implementation's behavior can reject every `List` or accept `MutableList` as `List` on the other. #17 tests both configurations. `typeInfo<T>()` resolves a platform type as `List<T>`, just as it resolves a platform type as non-null.

**Runtime values and copies.** Decoding builds a `java.util.ArrayList` for a `List` and a `java.util.LinkedHashMap` for a `Map`, which keeps the encoded entry order. Neither is wrapped in a read-only view. Kotlin's own read-only collections are ordinary JVM collections seen through a read-only interface, and a wrapper would cost an allocation per value without hiding anything Kotlin exposes. Flink's `ListSerializer` decodes into an `ArrayList` too. A collection type is not immutable (`isImmutableType` is false), because a producer may still hold the object it emitted and mutate it. `copy` builds a new `ArrayList` or `LinkedHashMap` in the same order and copies each element, key and value with that type's serializer, so a copy never shares the source collection object.

A `Map` value is written as its entries, and `copy` and the read rebuild it as a `LinkedHashMap` that looks keys up through their `equals()` and `hashCode()`. A map class with its own lookup rules loses them. A `TreeMap` with `String.CASE_INSENSITIVE_ORDER` holding only `"alice"` finds `"ALICE"`, and the rebuilt map doesn't, though both have one entry. The same goes for a natural-order `TreeMap` whose keys' `compareTo()` disagrees with `equals()`, for a `withDefault` map, whose default is a function that `getValue` calls, and for any of these behind `Collections.unmodifiableMap`. An `IdentityHashMap<String, Int>` can hold `"a"` twice, as two `String` objects, and the rebuilt map holds one entry. Only that last case changes the entry count, so a check after decoding misses the rest (all checked on Kotlin 2.4.20 and JDK 25).

So `serialize` and `copy` accept a `Map` value only if its runtime class is exactly one of the hash-based maps that the JDK and the Kotlin standard library build: `java.util.HashMap`, `java.util.LinkedHashMap`, the JDK's `Collections.emptyMap()`, `Collections.singletonMap()` and two `Map.of` classes, and Kotlin's `emptyMap()` and `buildMap` classes. Each looks a key up only through its `equals()` and `hashCode()`, so its keys are distinct and its lookups survive the rebuild. `mapOf`, `mutableMapOf`, `hashMapOf`, `toMap`, `associate`, `filter`, `mapValues`, `plus`, `buildMap`, `Map.of` and `Map.copyOf` all return one of these classes (checked on Kotlin 2.4.20 and JDK 25). Any other class, a subclass of `HashMap` included, fails with an error that names the declared type and the runtime class, before the map's size is written. On RocksDB the state `update()` fails, on the heap backend the checkpoint fails instead of completing, and on a network edge the emit fails, so no completed checkpoint holds such a map. A caller who accepts losing the class's behavior converts first, with `toMap()` or `LinkedHashMap(m)`. The private classes are taken once from instances that those factories return, not looked up by name. The per-record cost is one `getClass()` and a comparison against eight classes, with no reflection and no hashing.

`toSortedMap()` and `sortedMapOf()` return a `TreeMap`, so they're rejected. A natural-order `TreeMap` over `String` keys would survive the rebuild, but accepting it means checking its comparator and the key type's `compareTo()`, and no first-release use case needs it. Relaxing the check later changes no bytes. An access-ordered `LinkedHashMap` is accepted: its entries are written in their current order and come back in insertion order. A `List` isn't checked, because `java.util.List` defines `equals()`, `hashCode()` and `contains()` by element equality, and no JDK or Kotlin list class departs from that.

**Encoding.** A `List` is its size followed by its elements in list order. A `Map` is its size followed by its entries, key then value, in the map's iteration order. The size is a 4-byte big-endian `Int` from `DataOutput.writeInt`, the same framing as Flink's `ListSerializer` and `MapSerializer` and as [stable IDs](#enum-and-sealed-identity). Nothing measured shows that a variable-length size would be worth its edge cases. Each element, key and value is written by the codec for its full type, so a `User?` element is written exactly as #5 writes a `User?` anywhere else, and a collection adds no null bitmap of its own. A negative size fails the read. So does a map that decodes fewer distinct keys than its size, which would otherwise merge entries silently.

```text
List<User> = [User(1, "a"), User(2, "b")]
00 00 00 02                 size 2
<User(1, "a")>              #5's record bytes
<User(2, "b")>

List<User?> = [User(1, "a"), null]
00 00 00 02
<User(1, "a") as User?>     #5's nullable encoding, non-null
<null as User?>             #5's nullable encoding, null

Map<Long, User> = {7 → User(1, "a")}
00 00 00 01
<7 as Long>
<User(1, "a")>

emptyList<User>()
00 00 00 00
```

The bytes in angle brackets are [#5's](persisted-format.md#examples): `User(1, "a")` is `00 00 00 00 00 00 00 01 02 61`, and a nullable value is a `00` or `01` marker, then the value. [#17](https://github.com/Lychee-Technology/flinkt/issues/17) turns these examples into golden fixtures.

**One encoding, Flinkt's own `TypeInformation`.** A collection written as a record field and a collection that is itself the stream or state type use the same Flink-free codec. For a top-level `typeInfo<List<User>>()` or `typeInfo<Map<Long, User>>()`, each adapter supplies its own `TypeInformation`, `TypeSerializer` and snapshot over that codec, not Flink's `ListTypeInfo` or `MapTypeInfo`. Flink's `MapSerializer` writes its own null flag before every value and decodes into a `HashMap`, which drops the entry order (checked in 2.3.0). Flink's collection snapshots also resolve compatibility by Flink's rules, not by [Flinkt's](#schema-evolution). Using them for top-level collections would give one Kotlin type two persisted formats and two compatibility policies. Flink's `ListState` and `MapState` are unaffected. They're state structures whose element, key and value types come from `typeInfo`.

**Deterministic bytes.** A `List`'s bytes are deterministic when its elements' bytes are, because list equality includes order. A `Map`'s bytes aren't. Two equal maps built in different orders serialize differently, and the first release doesn't sort entries to prevent it. Sorting would mean serializing, buffering and ordering every key before writing, and every `Map` value would pay that cost to support map-valued keys that nothing needs. A `Map` is therefore never a key where a key context needs identical bytes for equal keys, and in the first release [no collection is a key](#keys). Golden fixtures build their maps in a fixed order.

**Snapshot.** The snapshot records the collection kind and the complete nested schema of the element type, or of the key and value types, with nullability and type arguments. A kind change, or a type change of the element, key or value, is incompatible. A `List` element and a `Map` value can migrate like any nested value, and a `Map` key never migrates, because two old keys could become one ([Schema evolution](schema-evolution.md#values)).

#### Map keys

Decoding and `copy` rebuild every value from its fields and keep no object identity. A record is rebuilt by calling its primary constructor with its decoded fields. Two fields that held one object come back as two equal objects, not as one. A decoded `LinkedHashMap` finds an entry only through the key's `equals()` and `hashCode()`, so a key has to come back equal to the key that was written, and its equality has to depend only on what's written. A `Map` key type is therefore a compile error, or a `typeInfo<T>()` error once type arguments are substituted, if it contains any of these:

- a modeled class that declares its own `equals()` or `hashCode()`, or inherits a final one from a superclass;
- a data class with a `var` constructor property;
- a `List` or a `Map`.

The check is recursive: it covers the key class, its components, type arguments and sealed subtypes.

Take `data class Ref(val id: Long)` with identity `equals()` and `hashCode()`, and a record with `lookup: Ref` and `values: Map<Ref, String>` whose one key is the `lookup` object. Before a round trip, `values[lookup]` finds the entry. After it, the map still has one entry, but the decoded `lookup` doesn't find it, and the decoded record isn't equal to the original. The same happens when the identity-equal class sits inside the key, as in `Map<Outer, String>` with `data class Outer(val ref: Ref)` whose equality is generated, which is why the check is recursive (modeled with codec-style reconstruction on Kotlin 2.4.20). The read's size check doesn't catch it, because the decoded keys are still distinct. A hand-written `equals()` that reads only persisted fields, such as a case-insensitive comparison of a `String` field, would survive decoding. Flinkt can't tell it apart from one that doesn't, so it's rejected too, for the same reason as a hand-written key in [Keys](#eligibility).

A `var` constructor property is rejected because the constructor might not store its argument unchanged. #11 allows `init` blocks, and KSP doesn't see them ([Rebuilding through the constructor](#rebuilding-through-the-constructor)). In `data class Key(var id: Int) { init { id++ } }`, `Key(0)` holds 1. Decoding calls `Key(1)`, which holds 2, so the decoded key isn't equal to the one written and a lookup with the original key finds nothing (checked on Kotlin 2.4.20). Generated equality doesn't help, because the components themselves changed. A `val` constructor property holds exactly its argument: Kotlin rejects reassigning it in an `init` block ("'val' cannot be reassigned", checked on Kotlin 2.4.20), and a constructor property can't have a custom getter. So a key whose data classes have only `val` constructor properties, over components that also come back equal, comes back equal. An `init` block that only checks its arguments, such as `require(id >= 0)`, is still allowed.

A collection is rejected anywhere in a key because its equality belongs to its runtime class, not to its declared type. Decoding always builds an `ArrayList` or a `LinkedHashMap`, which compare by content. A `List` key whose class declares its own `equals()` can make two keys unequal, so the outer map has two entries. They decode as two equal `ArrayList`s, and the read fails its distinct-key check on a checkpoint that was written without error. An `IdentityHashMap` key does the same (both modeled on Kotlin 2.4.20), and the runtime check on `Map` values ([Collections](#collections)) catches only that one. Checking the element types can't prevent it, and it's the reason a collection isn't a key in the [key contexts](#eligibility) either.

Every other modeled key keeps its meaning. Built-ins compare by value, including a `Float` or `Double`, whose boxed `equals()` compares the `floatToIntBits` or `doubleToLongBits` value that #5 writes. A nullable key decodes `null` as `null`. An enum constant, an `object` and a `data object` decode to the reading JVM's own instance, and the map is rebuilt in that JVM, so an identity hash code is that JVM's. Kotlin 2.4.20 doesn't let an enum or a `data object` declare `equals()` or `hashCode()`, and a value class can't declare them yet ([Eligibility](#eligibility)). A plain `object` can, and is then rejected as a key type like a data class.

This isn't a [key rule](#keys). A decoded `Map` is rebuilt in the reading JVM and looked up through `equals()`, never through its bytes, and no key hash code is persisted, so enums, objects, sealed types and nullable types stay allowed as `Map` keys. The check reads the same metadata that KSP records for key eligibility: whether a class's equality is compiler-generated, and whether its constructor properties are all `val`s. Outside a `Map` key and the key contexts, nothing a Flinkt codec builds depends on a value's `equals()`. Relaxing the rule later changes no bytes.

### Enum and sealed identity

Decided in [#20](https://github.com/Lychee-Technology/flinkt/issues/20).

Every enum constant and every sealed subtype that Flinkt serializes declares its persisted ID with `@FlinkId`. The enum, the sealed root, every nested sealed type and every subtype also carry `@FlinkType`, so a subtype is part of the generated type model because it's declared, never because Flinkt discovered it.

```kotlin
@FlinkType
enum class Color {
    @FlinkId(1) RED,
    @FlinkId(2) GREEN,
    @FlinkId(7) BLUE,
}

@FlinkType
sealed interface Event

@FlinkType
@FlinkId(1)
sealed interface UserEvent : Event

@FlinkType
@FlinkId(1)
data class UserCreated(val userId: Long) : UserEvent

@FlinkType
@FlinkId(2)
data class UserDeleted(val userId: Long) : UserEvent

@FlinkType
@FlinkId(2)
data object Shutdown : Event
```

An ID is never derived from declaration order, ordinal, source position or name. Order and ordinals change when a constant is inserted or the source is reordered. A name-derived ID would let a rename silently change what old bytes mean, and Flinkt prefers explicit incompatibility to silently reinterpreting state. With an explicit ID, a constant or subtype can be renamed, reordered or moved to another file without changing its bytes.

- **Encoding.** An ID is a positive `Int`, written as 4 big-endian bytes with `DataOutput.writeInt`. Zero and negative IDs are compile errors and stay reserved. An enum value is its ID and nothing else. A sealed value is its subtype's ID followed by the subtype's payload. For a data class that's #5's record bytes, for an `object` or `data object` it's nothing, and for a nested sealed type it's the nested union's own ID and payload. A fixed-width `Int` keeps generated code, fixtures and debugging simple, and four bytes per tag are acceptable until a measurement says otherwise.

  ```text
  Color.GREEN                     00 00 00 02
  Color.BLUE                      00 00 00 07
  Shutdown as Event               00 00 00 02
  UserCreated(42) as Event        00 00 00 01 | 00 00 00 01 | <UserCreated(42)>
  UserDeleted(42) as Event        00 00 00 01 | 00 00 00 02 | <UserDeleted(42)>
  UserDeleted(42) as UserEvent    00 00 00 02 | <UserDeleted(42)>
  ```

  Declaring `BLUE, RED, GREEN`, or `Shutdown` before `UserEvent`, gives the same bytes.
- **Nested hierarchies.** Each sealed type is its own tagged union with its own ID namespace. A nested sealed type has an ID in its parent's namespace and assigns IDs to its own direct subtypes, as `UserEvent` does above. A type that directly extends more than one modeled sealed type is a compile error in the first release. It would have one ID in two namespaces, and with nesting, two paths from the root.
- **Forms.** Supported: enums, including constants with bodies; sealed interfaces, and sealed classes without stored state of their own; data-class, `object`, `data object` and nested sealed subtypes. Decoding returns the declared constant or singleton instance. A constant with a body has its own JVM class, such as `Color$GREEN`, so generated code maps constants to IDs with a `when` over the constants, never by class or ordinal. A sealed class with stored state of its own fails [#11](https://github.com/Lychee-Technology/flinkt/issues/11)'s rule for inherited state. Whether generic sealed hierarchies are supported is [#22](https://github.com/Lychee-Technology/flinkt/issues/22)'s decision. If they are, IDs belong to declarations, not to type arguments.
- **Instance state.** An enum constant, an `object` and a `data object` are written as an ID alone, and decoding returns the instance that the reading JVM created when it initialized the class. Nothing that instance holds is in the bytes. A `var` set to 42 before the value is written reads as its initial value in the JVM that restores it, on an object and on an enum constant alike (checked on Kotlin 2.4.20, in two JVMs). A round trip inside one JVM returns the same instance, so it can't show the loss. State that wouldn't survive is therefore a compile error, as it is for a data class ([#11](https://github.com/Lychee-Technology/flinkt/issues/11)):
  - An `object` or `data object` subtype is a stateless marker. It follows #11's rule with no persisted properties at all: a property with a backing field, a delegated property, and state inherited from a superclass with fields of its own are errors. Computed properties and `const val`s are allowed. A subtype that carries data is a data class.
  - An enum's properties are never written. A `val`, as in `Color(val rgb: Int)`, belongs to the constant, and every JVM builds it from the declaration, so it's allowed. A stored `var` is an error, whether it has a backing field or a delegate, on the enum and in a constant's body.

  The two rules differ on `val`. An object subtype sits beside data-class subtypes in one hierarchy, and #11 already rejects a stored body property there, because KSP can't tell an initializer that every JVM evaluates alike from one that reads a clock. An enum's `val` has the same weakness, but it's how a constant carries fixed data, and rejecting it would reject most real enums. Neither rule sees inside a `val`: a mutable object it refers to isn't persisted either. Nor does any rule see interface delegation. In `object Deleg : Event, Counter by CounterImpl()`, KSP 2.3.12 shows no property and no trace of the delegate, yet the singleton holds a stateful `CounterImpl` (found in the #33 review). The same gap applies to #11's data-class rule. Relaxing either rule later changes no bytes.
- **Snapshot and unknown IDs.** The snapshot records the ID table: each ID with its constant's or subtype's name, and each sealed subtype's payload schema. Restoring against a different table is incompatible. That covers an ID added or removed, a name that the old table has under a different ID, and a subtype moved to another level. A change to a subtype's fields follows the record rules, so it can migrate as a nested value when the table itself is unchanged ([Schema evolution](schema-evolution.md#values)). Decoding an ID the current type doesn't have fails with an error naming the type and the ID. It never produces `null`, a default or another subtype. The two checks catch different failures. The snapshot comparison catches a changed declaration before any record is read, and the decode check catches bytes the snapshot didn't describe.
- **Names aren't identity, but a moved name is caught.** The table is keyed by ID, and a subtype is compared by its fields. Renaming or moving a constant or subtype that keeps its ID doesn't change the schema. The recorded names are compared for one thing only: a name that both tables have, under different IDs, is incompatible. Without that check, swapping or rotating IDs among constants that all still exist would leave the set of IDs unchanged. `@FlinkId(1) LOW, @FlinkId(2) HIGH` renumbered to `@FlinkId(2) LOW, @FlinkId(1) HIGH`, say to match a new declaration order, would restore and read every stored `LOW` as `HIGH`. Comparing fields doesn't catch it for sealed subtypes with the same fields, such as `UserCreated(userId)` and `UserDeleted(userId)`. A name that's new to the table is a rename. The cost is that a rename that reuses another live constant's old name in the same release, such as `B` to `C` and `A` to `B`, is incompatible. It takes two deployments: `B` to `C`, then `A` to `B`. The check reads only names the snapshot already records and changes no bytes. What it can't catch is an existing ID given to a name the old table doesn't have, such as two constants that swap IDs and both take new names. That looks exactly like a rename, so an ID is permanent and is never given to something else. Renaming the enum class or the sealed root itself follows #5's type-identity rule.
- **Compile-time errors.** A missing ID. A duplicate ID in one namespace, naming both declarations. An ID below 1. `@FlinkId` anywhere other than an enum constant or a subtype of a modeled sealed type. A subtype without `@FlinkType`, or one Flinkt can't model. A type that directly extends two modeled sealed types. An `object` or `data object` subtype with stored state. An enum with a stored `var`.
- **Keys.** A stable ID is a stable byte encoding, not a stable `hashCode()`. `Enum.hashCode()` and a plain `object`'s `hashCode()` are identity hash codes. A `data object`'s is the hash of its fully qualified name, such as `com.example.Event.Shutdown` (checked on Kotlin 2.4.20), so renaming it or moving it to another package or enclosing declaration changes it. [Keys](#keys) says where each can be a key.

## Keys

Decided in [#23](https://github.com/Lychee-Technology/flinkt/issues/23), implemented in [#32](https://github.com/Lychee-Technology/flinkt/issues/32).

Being serializable doesn't make a type a safe key, and a compatible serializer doesn't make a key safe after an upgrade. Flink uses a key's `hashCode()`, `equals()` and serialized bytes in ways that are persisted, and it uses them differently in three places. Flinkt decides for each place which Kotlin kinds it accepts. It can't change how Flink hashes, compares or stores a key.

| Context | Where Flinkt checks it | What Flink does with the key | Needs a `hashCode()` that's the same in every JVM | Needs equal keys to have identical bytes |
|---|---|---|---|---|
| `keyBy` partition key | the view's `keyBy`, when the job graph is built | `murmurHash(key.hashCode())` picks the key group, which checkpoints persist. Heap state is found by `equals()` and `hashCode()`, RocksDB state by the serialized key, and BATCH input is sorted by serialized key bytes | yes | yes |
| keyed `MapState` user key | `runtimeContext.mapState<K, V>()`, when the state is bound in `open()` | addresses entries inside one partition key's state. The heap backend rebuilds a Java map in the restoring JVM. RocksDB addresses and orders entries by the serialized user key. No part in key groups | no | yes, on RocksDB |
| broadcast-state map key | nowhere yet: broadcast is raw Flink | operator state. Every parallel instance holds the whole map, and restore reads each entry into a Java map in the restoring JVM | no | no |

Every context needs `equals()` and `hashCode()` to agree within one JVM. Where bytes matter, they also have to agree with `equals()` both ways. Equal keys with different bytes are separate RocksDB entries, and unequal keys with the same bytes are one entry. Checked in the 2.3.0 sources (`KeyGroupRangeAssignment`, `SortingDataInput`, `DefaultOperatorStateBackend`, `OperatorStateRestoreOperation`). The earlier review of [#25](https://github.com/Lychee-Technology/flinkt/issues/25) checked the restore paths in 1.20.5 as well.

### Eligibility

| Kind | `keyBy` partition key | keyed `MapState` user key | broadcast-state key |
|---|---|---|---|
| non-null `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Char`, `String` | allowed | allowed | allowed |
| value class | allowed if eligible | allowed if eligible | allowed if eligible |
| `@FlinkType` data class, including a generic one with its type arguments | allowed if eligible | allowed if eligible | allowed if eligible |
| enum | rejected | allowed, subject to evidence | allowed, subject to evidence |
| `object`, `data object` | rejected | rejected | rejected |
| sealed hierarchy | rejected | rejected | rejected |
| `List`, `Map` | rejected | rejected | rejected |
| nullable type `K?` | rejected | rejected | rejected |

A data class or value class is eligible in a context only if all of these hold:

- it's non-null;
- its `equals()` and `hashCode()` are the ones the Kotlin compiler generates. The class declares neither, and doesn't inherit a final one from a superclass, which would replace the generated pair. Kotlin 2.4.20 doesn't let a value class declare them ("reserved for future releases"). If a later Kotlin does, a value class that declares them is ineligible too;
- for a data class, every constructor property is a `val`. A value class's underlying property always is;
- every persisted component is eligible in the same context. For a data class that means its constructor properties, for a value class its underlying value, in both cases after substituting type arguments;
- for a value class, #18's representation of it is deterministic.

The rule is recursive, and one ineligible component makes the whole key ineligible. `UserKey(val tenant: TenantId, val id: Long)` is a partition key only if `TenantId` and `Long` are, and `Envelope<T>` is decided separately for each `T`. A data class with an enum property can be a `MapState` user key but not a partition key. A nullable component makes a key ineligible in every context, just as a nullable key does. KSP records in each generated type's metadata whether its equality is compiler-generated and whether its constructor properties are all `val`s, so the check reads metadata and uses no reflection.

The reasons:

- **Proof, not trust.** Every eligible kind is a final class, so the `hashCode()` that runs is the one Flinkt checked. A compiler-generated `hashCode()` combines the components' hash codes, and for the built-ins those are specified by the Java SE API and the same in every JVM. How they're combined is up to the Kotlin compiler that builds the key class ([What a snapshot can't see](#what-a-snapshot-cant-see)). Equality is component-wise, and each component's bytes are deterministic, so equal keys have equal bytes and heap and RocksDB address the same entries. Flinkt can't reason about a hand-written `equals()` or `hashCode()`. It can't tell whether one is stable across JVMs, agrees with the bytes, or will stay the same in the next release. A key that has one is rejected, rather than accepted with state that a code change can make unreachable.
- **A restored key is the key that was written.** A stable hash and deterministic bytes don't help if decoding builds a different key. The codec rebuilds a data-class key through its primary constructor, and #11 lets an `init` block run there and change a `var` constructor property. `data class Key(var id: Int) { init { id++ } }` is written as `id == 1` and decodes as `id == 2`. Wherever Flink rebuilds a map from decoded keys, as the heap backend and broadcast state do on restore, the original key no longer finds its state, with an unchanged schema and no error. A `val` can't be reassigned there, so a key of `val`s over eligible components decodes equal to the one written ([Map keys](#map-keys)).
- **Enums: stable bytes, unstable hash.** #20's ID makes an enum's bytes stable, but `Enum.hashCode()` is an identity hash code. It can differ in the restoring JVM and move the key group, which is why Flink itself rejects `EnumTypeInfo` as a `keyBy` key. A `MapState` user key and a broadcast-state key never reach key-group assignment. On restore, the heap backend and broadcast state rebuild their maps in the new JVM, where the decoded constant is the canonical instance, and RocksDB compares the ID bytes. So an enum is allowed in those two contexts once #23's feasibility test shows, on each target line, that every entry restored in a second JVM is found through its constant: heap and RocksDB `MapState`, and broadcast state. Otherwise enums are rejected there too.
- **Objects and sealed hierarchies.** A plain `object`'s hash code is an identity hash code, and a `data object`'s is its qualified name's, which a rename or a package move changes. A single-valued key is also of no use. As a partition key it sends every record to one key group, and a map with one possible key is a `ValueState`. Sealed hierarchies mix data-class and object subtypes, and no first-release use case needs one as a key.
- **Collections.** A `List<T>` value can be any `java.util.List` implementation at runtime, with whatever `hashCode()` it has, so nothing about it can be proved before it's serialized. A `Map`'s bytes also depend on its iteration order ([Collections](#collections)).
- **Nullable keys.** Flink rejects a null partition key per record at runtime (`KeyGroupRangeAssignment`: "Assigned key must not be null!"), so Flinkt rejects `K?` when the graph is built instead. For `MapState` and broadcast keys, and for nullable components, the first release keeps the same rule.

Apart from the enum partition key, every rejection can be relaxed later without changing any existing key's bytes or hash code. Tightening a rule after users have keyed state by a type would strand that state, so the first release starts narrow.

### Deterministic bytes

The partition-key and `MapState` rules need equal keys to have equal bytes and unequal keys different bytes. They rely on these formats:

- Flink's own serializers for the built-ins, which `typeInfo` returns for them ([#7](https://github.com/Lychee-Technology/flinkt/issues/7)). Their bytes are Flink's, not Flinkt's. `FloatSerializer` and `DoubleSerializer` call the view's `writeFloat` and `writeDouble`, so a `Float` or `Double` key has one NaN encoding only on a view that canonicalizes ([Generated code and the adapter](#generated-code-and-the-adapter)). Flink builds RocksDB keys (`SerializedCompositeKeyBuilder`) and BATCH sort keys (`SortingDataInput`) in a `DataOutputSerializer`, which does (checked on 1.20.5, 2.2.1 and 2.3.0). The rule for these two key types rests on that Flink behavior, which Flinkt doesn't control.
- #5's records, which write fields in a fixed order, so a record's bytes are deterministic when its fields' bytes are ([Deterministic bytes](persisted-format.md#deterministic-bytes)). Two properties of #5's format are therefore requirements. Every NaN has one encoding on every view, matching data-class equality: a codec converts a `Float` or `Double` with `floatToIntBits` or `doubleToLongBits` and writes the result with `writeInt` or `writeLong`. It never calls `writeFloat` or `writeDouble`, which would leave the NaN's bytes to the view. Every `String` is encoded losslessly. `String.toByteArray(Charsets.UTF_8)` isn't lossless: it writes an unpaired surrogate such as `"\uD800"` as `?`, the same byte as `"?"` (checked on JDK 25), which would make two different keys one.
- #20's IDs, one fixed encoding per constant.
- #18's value-class representation, once #18 decides it.

Maps aren't deterministic ([Collections](#collections)), and no collection is a key.

### Where the rules apply

Until #32 implements these rules, the view's `keyBy` applies an interim policy ([#9](https://github.com/Lychee-Technology/flinkt/issues/9)) that is narrower than both bullets below. Flink's `keyBy` doesn't read `isKeyType()`, and `KeyedStream.validateKeyType` rejects only POJOs without `hashCode()`, arrays and `EnumTypeInfo` (checked in the 1.20.5 and 2.3.0 sources), so Flink would accept any `TypeInformation` that Flinkt later builds, including one for a key whose `hashCode()` differs between JVMs. The interim `keyBy` therefore accepts only the non-null `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Char` and `String`, and refuses every other key type before it calls Flink, naming it. The reified overload checks `K`'s `KType` before it looks up `typeInfo<K>()`, so a type that `typeInfo` resolves in a later release is still refused. The overload that takes a `TypeInformation` accepts only Flink's `BasicTypeInfo` for those types: the constant that `typeInfo` gives, or a copy equal to it and of the constant's own class, such as the one Java serialization gives back, because a `TypeInformation` is `Serializable`. `BasicTypeInfo.equals` compares the boxed class, the serializer and the comparator class, so an equal copy gives a key Flink's bytes and comparisons. A subclass is refused even when it's equal, since `BasicTypeInfo.canEqual` accepts every subclass and a subclass can create another serializer. The overload names a refused `TypeInformation` by its `toString()` and its class. The set is listed, so a built-in mapping that `typeInfo` adds later doesn't join it by itself. Flink's own rules stay reachable through `asFlink().keyBy(…)`.

- **The view's `keyBy` with a reified key type** applies the partition-key rules to the key's full Kotlin type when the job graph is built, before it calls Flink. The `TypeInformation` it passes is in the partition-key role, which the snapshot records, so a later constructor reorder of the key fails the restore ([Schema evolution](schema-evolution.md#keys)).
- **The view's `keyBy` with an explicit `TypeInformation`** applies them, and passes Flink the same type in the partition-key role, if the `TypeInformation` is one of Flinkt's own classes, which carry the canonical Kotlin type. Any other `TypeInformation`, whether it comes from Flink, a library or the caller, is an intentional escape hatch. Flinkt checks nothing, Flink's `keyBy` and its own validation apply, and the caller gets Flink's guarantees instead of Flinkt's key-safety guarantee. It's the same exit as passing a `TypeInformation` for a type Flinkt doesn't model, or calling `asFlink().keyBy(…)`. `typeInfo<Long>()` returns Flink's `BasicTypeInfo` (#7), so it counts as foreign, and Flink accepts every built-in that Flinkt would. Policing a foreign `TypeInformation` would add little safety, because the caller can always use raw Flink.
- **A Flinkt `TypeInformation` on the raw path** gets neither check. Flink's `keyBy` validation (`KeyedStream.validateKeyType`) recognizes only Flink's own classes. It rejects `EnumTypeInfo` and the array type infos, checks a `PojoTypeInfo` for a declared `hashCode()`, and looks inside tuples. A Flinkt `TypeInformation` is none of these, so Flink accepts it unchecked. `asFlink().keyBy(selector, typeInfo<Priority>())` builds and runs with an enum partition key, where `keyBy(selector)` and `keyBy(selector, EnumTypeInfo)` throw `InvalidProgramException` (checked on 1.20.5, 2.2.1 and 2.3.0). Keyed `connect`, joins and `coGroup` have no view yet, so passing `typeInfo<K>()` for their key is the natural way to avoid Kryo, and it gives up both checks. [#41](https://github.com/Lychee-Technology/flinkt/issues/41) tracks a public function that applies the partition-key rules on that path.
- **`runtimeContext.mapState<K, V>()`** applies the `MapState` user-key rules when it binds the state in `open()`, before the first record.
- **`mapStateDescriptor<K, V>()`** applies no key rule. Its descriptor can become keyed `MapState` or broadcast state, and the helper can't tell which. It does create `K`'s serializer in the map-key role, which is true in both, so a later restore never migrates the key ([Schema evolution](schema-evolution.md#keys)). A descriptor passed to Flink directly gets Flink's checks.
- **Broadcast-state keys** have no enforcement point until a view offers broadcast. The README states their rules.

A rejection names the canonical Kotlin type, the context, the component that makes a composite ineligible, and the reason.

`isKeyType()` and `isSortKeyType()` return `false` for every Flinkt `TypeInformation`, and Flinkt supplies no `TypeComparator`. Flink defines a key type as hashable and comparable, and nothing in the first release compares Flinkt keys with a comparator. DataStream `keyBy` never reads `isKeyType()` (`KeyedStream.validateKeyType`), and BATCH sorting compares serialized bytes (`SortingDataInput`). The Flink paths that do read the flags are `sortPartition` by `KeySelector` and field-expression keys, on 1.20, 2.2 and 2.3 (checked in the jars). `sortPartition` by `KeySelector` types the key with `TypeExtractor.getKeySelectorTypes`, so it sees a Flinkt `TypeInformation` only when the selector implements `ResultTypeQueryable`. Otherwise it sees a `GenericTypeInfo`: it accepts a key class that implements `Comparable`, sorted through Kryo, and rejects one that doesn't, without reading Flinkt's flags. A selector that implements `ResultTypeQueryable` with a Flinkt `TypeInformation`, and a field-expression sort on a stream typed by one, are rejected with Flink's own `InvalidProgramException` instead of looking for a comparator that doesn't exist (checked on 1.20.5, 2.2.1 and 2.3.0). The [comparator contract](testing.md#comparator-and-key-semantics) applies if a later release adds comparators.

### What a snapshot can't see

A serializer snapshot compares schemas, not method bodies. Suppose a data class that keys persisted state gains an `override fun hashCode()`. Its schema is unchanged, but its key groups move. A changed `equals()` changes which keys are the same. Either is a breaking state change, even though the serialized schema is identical.

Flinkt doesn't try to detect this in the snapshot. The eligibility rules admit only compiler-generated equality over components whose hash codes are specified, so an eligible key's behavior follows from its schema and its constructor order. The order matters because the generated `hashCode()` combines components in constructor order, while #5's canonical field order keeps it out of the bytes. So the snapshot records constructor order, and a reorder in a partition key is incompatible ([Schema evolution](schema-evolution.md#keys)). The same edit also makes the type ineligible, so the upgraded job's `keyBy` through the view fails when its graph is built. Through `asFlink()` or a foreign `TypeInformation`, nothing checks it. Cross-release savepoint tests ([#29](https://github.com/Lychee-Technology/flinkt/issues/29)) are the system-level backstop.

One dependency remains. The formula of the compiler-generated `hashCode()` is Kotlin compiler behavior, not a documented guarantee. The compiler it depends on is the one that builds the user's key class, not the one that builds Flinkt. For a data class it's `31 * h + component.hashCode()`, so `UserKey(1, 2)` hashes to 33. For a value class it's the underlying value's hash code. Both were checked on Kotlin 2.4.20. The formula has changed before, for a component kind on the eligible list: compiled by Kotlin 1.4.32, a `Boolean` component contributes 1 or 0, and compiled by 2.4.20 it contributes `Boolean.hashCode()`, 1231 or 1237. So `data class BoolKey(val flag: Boolean, val id: Long)` gives `BoolKey(true, 1).hashCode()` 32 on one and 38162 on the other, on the same JDK. The other built-in kinds and `UserKey(1, 2)` hashed alike on both (measured in the #33 review). If a later Kotlin changes the formula again, a user who upgrades Kotlin moves existing keys to other key groups, with no schema change and no error. Kotlin 1.4.32 is historical evidence of that, not a supported compiler: Flinkt supports Kotlin 2.4.0 and later ([Kotlin compiler compatibility](kotlin-compatibility.md)), and 2.4.0 through 2.4.21 gave identical hash codes and key groups for every eligible component kind. #32 therefore pins the hash codes and key groups of at least one key per eligible component kind, `Boolean` included, and checks every supported key shape, over boundary values, against an independently specified formula, with the key classes compiled by Kotlin 2.4.0 and by the newest tested version. A Kotlin release is listed as tested only after those checks pass with it, so an upgrade that changes the formula is never treated as a compatible keyed-state upgrade. That is tested evidence for the shapes and values the checks cover, not a proof for every key. The checks can't cover a compiler Flinkt's CI never runs, and Flinkt can't detect which compiler built a key class ([Enforcement and its limits](kotlin-compatibility.md#enforcement-and-its-limits)).

## Schema evolution

Decided in [#25](https://github.com/Lychee-Technology/flinkt/issues/25). [schema-evolution.md](schema-evolution.md) is the decision record: the full matrix, the key roles, the stages, and the Flink restore paths it relies on.

For ordinary persisted values, the target is the evolution Flink's `PojoSerializer` offers. A removed field is dropped. An added field migrates when Flinkt can construct a legal value for it, which a Kotlin non-null field doesn't get for free. Both work recursively inside nested records, `List` elements, `Map` values, generic arguments and sealed payloads. A field type change and a class rename are incompatible. Keys don't evolve structurally in any role, and neither does anything under a `Map` key.

```text
steady state
    current generated codec  →  compact current bytes

restore
    old snapshot's schema  +  current generated schema  →  plan

    as-is               the current codec reads the old bytes
    after migration     the old-layout serializer reads the old bytes into current values,
                        and the new serializer writes them in the current layout
    incompatible        restore fails explicitly, before any value is read wrongly
```

Records stay compact, and the snapshot carries the schema. The first snapshot Flinkt writes already records a symbolic, recursive schema, so a later release can read first-release bytes without the classes that wrote them. A Flink-free planner compares the old and current schemas and produces one plan. `resolveSchemaCompatibility` answers Flink from it, and the old-layout serializer executes it, so a compatibility answer can't promise a migration that the reader doesn't perform. Migrated values are built through generated constructors, without reflection.

The first decision ([#33](https://github.com/Lychee-Technology/flinkt/pull/33)) was strict: every structural change incompatible. That stays the behavior for each transition until the stage that enables it lands with its evidence ([Stages](schema-evolution.md#stages)).

## Open questions

These are decided with the serializer implementation, because each one fixes a persisted format:

- **Value classes.** When a value class may use its underlying type's serializer ([#18](https://github.com/Lychee-Technology/flinkt/issues/18)). [Schema evolution](#schema-evolution) and [Keys](#keys) already fix some of it. The direction is that `UserId(val value: Long)` writes exactly the bytes a `Long` writes, while its schema records the wrapper's identity and the underlying schema. So `Long`, `UserId` and `OrderId` stay three schemas with identical bytes, and switching between them, or changing the underlying type, is incompatible. A value class is a key only if its representation is deterministic.
- **Migration defaults.** Which added fields get a migration default, and how a declaration states one ([#37](https://github.com/Lychee-Technology/flinkt/issues/37)).

These are sequencing questions that don't affect the architecture:

- **Views after the first slice.** Which Flink types get views next: windows, `connect`, broadcast, joins. Which type-introducing methods of the existing views get hand-written versions, such as `getSideOutput`. The generator refuses these, so until then they're reached through `asFlink()`. A side output's element type comes from its `OutputTag`, not from the call. `OutputTag(id)` takes that type from Flink's `TypeExtractor` on 1.20 and 2.3, and `OutputTag(id, typeInfo<T>())` doesn't, so the design has to cover how tags are built.
- **Typed sources.** Whether typed-source helpers are worth adding, given that Flink's typed `fromSource` is `@Experimental`.
- **Overload selection.** Letting a view forward one overload of a name, which `broadcast()` needs.
- **KDoc for generated forwarders.** The first version has none. One option is to link each forwarder to the Flink method it calls, with Dokka external links to the Javadoc Flink publishes for each line. The other is to copy Flink's Javadoc text. Copying adds attribution obligations, because that text is under the Apache License 2.0 with a NOTICE file and Flinkt is MIT-licensed. A copy also describes the Java method's return type rather than the view's.
