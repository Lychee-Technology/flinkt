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

These rules keep the [user-facing promises](../README.md#flinkt-streams) true:

1. **A view never constructs a Flink stream object.** It holds the object it was given or the one a Flink call returned. Nothing else in Flinkt holds Flink's per-object state, so there's nothing to copy or keep in sync. Wrapping an object reads nothing from it, not even its type, whether the object entered with `.flinkt()` or a Flink call returned it. `Transformation.getOutputType()` marks the type as used, and a later `returns()` on the Flink object would then throw.
2. **A view method makes one call to the Flink method of the same name on the held object, and wraps the object that call returns.** Flink's validation, partitioning and operator selection apply unchanged. A fluent method wraps what Flink returned instead of assuming Flink returned `this`.
3. **A method that introduces an element type is `inline` with a reified type parameter, and it takes Flink's own function type.** A Kotlin lambda converts to that type, and a function object passes through unchanged, so both get their result type from the static type at the call site. Each such method has a non-inline overload that takes the `TypeInformation` explicitly, for generic code where the type isn't known.
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
public open class FlinktDataStream<T> @PublishedApi internal constructor(
    @PublishedApi internal open val flink: DataStream<T>,
) : FlinktDataStreamForwarders<T> {
    override fun asFlink(): DataStream<T> = flink

    public inline fun <reified R> map(fn: MapFunction<T, R>): FlinktSingleOutputStreamOperator<R> = ...
}
```

When Flinkt builds an adapter, `flinkt-view-codegen` reads the listed methods from that line's Flink classes on the compile classpath and generates a `sealed interface FlinktDataStreamForwarders<T>`. The view implements it, so the forwarded methods are members, and users need no imports. Each generated method follows rule 2: it calls the Flink method on `asFlink()` and wraps a returned stream in its view. A parameter that takes a Flink stream with a view takes the view, including varargs, so `union` accepts views. A Java collection comes back as the read-only Kotlin type.

The generator follows one principle: a forwarder carries Flink's API as Flink declares it. That covers overloads, parameter names, and stability status. The generator only refuses what would break a view rule. Where Flinkt is stricter than Flink, the reason is type propagation, not taste.

| The listed method | Result |
|---|---|
| doesn't exist on the line being built | build error naming the method and the Flink version |
| returns a type built from a method type parameter (`map`, `process`, `connect`, `getSideOutput`) | build error: write it by hand with a reified type parameter |
| returns a Flink stream or builder type that has no view (`BroadcastStream`, `WindowedStream`) | build error: forwarding it would let a chain leave the view without `asFlink()` |
| is `@Internal` (for example `getTransformation`) | build error: `@Internal` isn't public API in Java either |
| is deprecated | forwarded and annotated with Kotlin's `@Deprecated`, so a Kotlin caller gets the warning a Java caller gets |
| uses a deprecated type in its signature, but isn't deprecated itself (1.20's `sinkTo` for the legacy `Sink`) | forwarded unannotated, as Flink declares it |
| is `@Experimental` (for example 2.x `enableAsyncState`) | forwarded and annotated with `@ExperimentalFlinkApi`, a Kotlin `@RequiresOptIn` marker at warning level, so a call without `@OptIn` warns |

The opt-in marker implements the [dependency policy](flink-compatibility.md#experimental): functionality built on an experimental Flink API is itself explicitly experimental.

A name is listed once and covers all of its overloads, so each overload has to pass. `broadcast()` returns a stream, but `broadcast(MapStateDescriptor...)` returns `BroadcastStream`, so `broadcast` can't be listed until the generator can select a single overload.

The generator runs against every line's classes. A listed method is therefore checked on every line, and a method that exists on only some lines fails the build on the others. Line-specific methods go in the line's own source set. Deprecated overloads that only one line has are Flink's own difference between lines. They appear as listed per-line differences in the cross-adapter API comparison. In the spike, 1.20 had six such overloads.

#### Parameter names

Flink's jars have no `MethodParameters` attribute, so the compiler and KSP see `p0`, `p1`. Kotlin allows named arguments, which makes parameter names part of the source API. The names are still in the jars: Flink compiles with debug information, and each method's LocalVariableTable names its parameters. The forwarders use Flink's own names:

- **Extraction.** A script in Flinkt's build reads each line's class files with ASM. It takes each public method's JVM descriptor and its parameter names from the LocalVariableTable, and writes one row per method to a per-adapter `flink-api.tsv`. It prefers a `MethodParameters` attribute if Flink ever adds one. The file is checked in and records the Flink version it came from. The script reads the same jars the adapter compiles against, so each descriptor is the class file's own. It fails if a public method has no parameter names, which would mean Flink stopped compiling with debug information.
- **Lookup.** KSP receives the file as a tracked Gradle input, so editing or regenerating it reruns generation. It looks up each forwarded method by its descriptor. A missing entry, or a file extracted from a different Flink version than the one on the classpath, fails the build. KSP's `Resolver.mapToJvmSignature` omits the array marker of a Java varargs parameter, so the generator corrects that before the lookup.
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
- the type's persisted schema (#5);
- a migration constructor, which builds the current value from the values a migration resolved and calls the primary constructor directly ([Constructing current values](schema-evolution.md#constructing-current-values));
- one `GeneratedTypeModule` per compilation, registered in `META-INF/services` and discovered with `java.util.ServiceLoader`.

Generated code mentions no Flink type, so it doesn't change with the Flink line. Flink's `DataInputView` and `DataOutputView` implement `DataInput` and `DataOutput`, so the adapter passes them through without copying.

Generated code never subclasses a Flink class. The Flink protocol classes differ between lines: in 1.20, `TypeInformation.createSerializer(ExecutionConfig)` is the abstract factory and `TypeSerializerSnapshot.resolveSchemaCompatibility(snapshot)` has a default. In 2.x, `createSerializer(SerializerConfig)` is the abstract factory and `resolveSchemaCompatibility` is abstract. A generated `TypeInformation` would therefore need one build per line. Instead, each adapter has one `TypeInformation`, one `TypeSerializer` and one `TypeSerializerSnapshot` implementation shared by every generated type. That keeps comparators and snapshot logic in one place. Flink writes the snapshot class's fully-qualified name into every checkpoint and instantiates it by name on restore, so that class has the same name on every adapter line, and no per-line subclass or wrapper becomes the class Flink records. Flink creates serializers from `TypeInformation` with its own configuration, and Flinkt doesn't keep global serializer instances.

A consuming module reuses generated metadata from its dependencies and doesn't regenerate code for classes it doesn't own.

## State helpers

Two helper families have different names because they return different things:

- `valueStateDescriptor<T>(name)`, `listStateDescriptor<T>(name)` and `mapStateDescriptor<K, V>(name)` build Flink descriptors. Users need descriptors to configure TTL and to pass to Flink APIs that take one, such as broadcast state.
- `runtimeContext.valueState<T>(name)`, `listState<T>(name)` and `mapState<K, V>(name)` bind state through `getState`, `getListState` and `getMapState` with such a descriptor, and return Flink's `ValueState`, `ListState` or `MapState`.

Binding stays a visible call in `open()`. A property delegate could hide it, but then the point where state becomes available would be less obvious. Flink 2.x's asynchronous state API (`org.apache.flink.api.common.state.v2`, `@Experimental`) isn't covered yet.

## Persisted formats

Generated serializers write bytes that users' checkpoints and savepoints keep, and the snapshot Flink stores beside them decides whether a later job may read those bytes. Record layout, null representation, string encoding and snapshot format are still open ([#5](https://github.com/Lychee-Technology/flinkt/issues/5)), and so is the value-class representation ([#18](https://github.com/Lychee-Technology/flinkt/issues/18)). [What the first snapshot must carry](schema-evolution.md#what-the-first-snapshot-must-carry) constrains both. The decisions below build on #5: where an example shows a record, a scalar or a nullable value in angle brackets, its bytes are #5's.

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

An unsupported collection type fails explicitly, at compile time as a `@FlinkType` field and in `typeInfo<T>()` as a top-level type. Elements, keys and values can be any modeled type, nullable ones included, so `List<User?>` and `Map<UserId, List<Event?>>` work once their element types do.

`List` and `Map` cover the README's cases: ordered homogeneous values, key-value structures, nested generics and nullable nested values. `Set` is left out because equal sets have no natural byte order. `setOf(a, b)` equals `setOf(b, a)`, but writing each in iteration order gives different bytes. Fixing that means choosing an order for arbitrary element types and buffering and sorting every set's elements, and no first-release use case needs it. The mutable interfaces are rejected rather than treated as their read-only counterparts. Treating them that way would give two types that Kotlin distinguishes one `TypeInformation`, and it would invite mutating values that Flink may keep in heap state or, with object reuse, pass to chained operators without copying. On the JVM, `MutableList<T>` and `List<T>` are both `java.util.List`, and `typeOf` distinguishes them only through `KType.equals`. Their `toString()` and classifier are the same (checked on Kotlin 2.4.20). A Java collection that Kotlin sees as a platform type, `(Mutable)List<T!>!`, equals neither and passes a naive mutability check as mutable. `typeInfo<T>()` resolves it as `List<T>`, just as it resolves a platform type as non-null.

**Runtime values and copies.** Decoding builds a `java.util.ArrayList` for a `List` and a `java.util.LinkedHashMap` for a `Map`, which keeps the encoded entry order. Neither is wrapped in a read-only view. Kotlin's own read-only collections are ordinary JVM collections seen through a read-only interface, and a wrapper would cost an allocation per value without hiding anything Kotlin exposes. Flink's `ListSerializer` decodes into an `ArrayList` too. A collection type is not immutable (`isImmutableType` is false), because a producer may still hold the object it emitted and mutate it. `copy` builds a new `ArrayList` or `LinkedHashMap` in the same order and copies each element, key and value with that type's serializer, so a copy never shares the source collection object.

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

The bytes in angle brackets are fixed when #5 is decided. [#17](https://github.com/Lychee-Technology/flinkt/issues/17) turns these examples into golden fixtures.

**One encoding, Flinkt's own `TypeInformation`.** A collection written as a record field and a collection that is itself the stream or state type use the same Flink-free codec. For a top-level `typeInfo<List<User>>()` or `typeInfo<Map<Long, User>>()`, each adapter supplies its own `TypeInformation`, `TypeSerializer` and snapshot over that codec, not Flink's `ListTypeInfo` or `MapTypeInfo`. Flink's `MapSerializer` writes its own null flag before every value and decodes into a `HashMap`, which drops the entry order (checked in 2.3.0). Flink's collection snapshots also resolve compatibility by Flink's rules, not by [Flinkt's](#schema-evolution). Using them for top-level collections would give one Kotlin type two persisted formats and two compatibility policies. Flink's `ListState` and `MapState` are unaffected. They're state structures whose element, key and value types come from `typeInfo`.

**Deterministic bytes.** A `List`'s bytes are deterministic when its elements' bytes are, because list equality includes order. A `Map`'s bytes aren't. Two equal maps built in different orders serialize differently, and the first release doesn't sort entries to prevent it. Sorting would mean serializing, buffering and ordering every key before writing, and every `Map` value would pay that cost to support map-valued keys that nothing needs. A `Map` is therefore never a key where a key context needs identical bytes for equal keys, and in the first release [no collection is a key](#keys). Golden fixtures build their maps in a fixed order.

**Snapshot.** The snapshot records the collection kind and the complete nested schema of the element type, or of the key and value types, with nullability and type arguments. A kind change, or a type change of the element, key or value, is incompatible. A `List` element and a `Map` value can migrate like any nested value, and a `Map` key never migrates, because two old keys could become one ([Schema evolution](schema-evolution.md#values)).

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
- **Snapshot and unknown IDs.** The snapshot records the ID table: each ID with its constant's or subtype's name, and each sealed subtype's payload schema. Restoring against a different table is incompatible. That covers an ID added, removed or changed, and a subtype moved to another level. A change to a subtype's fields follows the record rules, so it can migrate as a nested value when the table itself is unchanged ([Schema evolution](schema-evolution.md#values)). Decoding an ID the current type doesn't have fails with an error naming the type and the ID. It never produces `null`, a default or another subtype. The two checks catch different failures. The snapshot comparison catches a changed declaration before any record is read, and the decode check catches bytes the snapshot didn't describe.
- **Names aren't identity.** The table is compared by ID, and a subtype by its fields, not by its name. Renaming or moving a constant or subtype that keeps its ID doesn't change the schema. The flip side is that Flinkt can't tell a rename from an existing ID given to a different constant, and old state would then decode as the new constant. An ID is permanent and is never reused for something else. Renaming the enum class or the sealed root itself follows #5's type-identity rule.
- **Compile-time errors.** A missing ID. A duplicate ID in one namespace, naming both declarations. An ID below 1. `@FlinkId` anywhere other than an enum constant or a subtype of a modeled sealed type. A subtype without `@FlinkType`, or one Flinkt can't model. A type that directly extends two modeled sealed types.
- **Keys.** A stable ID is a stable byte encoding, not a stable `hashCode()`. `Enum.hashCode()` and a plain `object`'s `hashCode()` are identity hash codes. A `data object`'s is the hash of its simple name (checked on Kotlin 2.4.20), which a rename changes. [Keys](#keys) says where each can be a key.

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
- every persisted component is eligible in the same context. For a data class that means its constructor properties, for a value class its underlying value, in both cases after substituting type arguments;
- for a value class, #18's representation of it is deterministic.

The rule is recursive, and one ineligible component makes the whole key ineligible. `UserKey(val tenant: TenantId, val id: Long)` is a partition key only if `TenantId` and `Long` are, and `Envelope<T>` is decided separately for each `T`. A data class with an enum property can be a `MapState` user key but not a partition key. A nullable component makes a key ineligible in every context, just as a nullable key does. KSP records in each generated type's metadata whether its equality is compiler-generated, so the check reads metadata and uses no reflection.

The reasons:

- **Proof, not trust.** Every eligible kind is a final class, so the `hashCode()` that runs is the one Flinkt checked. A compiler-generated `hashCode()` combines the components' hash codes, and for the built-ins those are specified by the Java SE API and the same in every JVM. Equality is component-wise, and each component's bytes are deterministic, so equal keys have equal bytes and heap and RocksDB address the same entries. Flinkt can't reason about a hand-written `equals()` or `hashCode()`. It can't tell whether one is stable across JVMs, agrees with the bytes, or will stay the same in the next release. A key that has one is rejected, rather than accepted with state that a code change can make unreachable.
- **Enums: stable bytes, unstable hash.** #20's ID makes an enum's bytes stable, but `Enum.hashCode()` is an identity hash code. It can differ in the restoring JVM and move the key group, which is why Flink itself rejects `EnumTypeInfo` as a `keyBy` key. A `MapState` user key and a broadcast-state key never reach key-group assignment. On restore, the heap backend and broadcast state rebuild their maps in the new JVM, where the decoded constant is the canonical instance, and RocksDB compares the ID bytes. So an enum is allowed in those two contexts once #23's feasibility test shows, on each target line, that every entry restored in a second JVM is found through its constant: heap and RocksDB `MapState`, and broadcast state. Otherwise enums are rejected there too.
- **Objects and sealed hierarchies.** A plain `object`'s hash code is an identity hash code, and a `data object`'s is its name's, which a rename changes. A single-valued key is also of no use. As a partition key it sends every record to one key group, and a map with one possible key is a `ValueState`. Sealed hierarchies mix data-class and object subtypes, and no first-release use case needs one as a key.
- **Collections.** A `List<T>` value can be any `java.util.List` implementation at runtime, with whatever `hashCode()` it has, so nothing about it can be proved before it's serialized. A `Map`'s bytes also depend on its iteration order ([Collections](#collections)).
- **Nullable keys.** Flink rejects a null partition key per record at runtime (`KeyGroupRangeAssignment`: "Assigned key must not be null!"), so Flinkt rejects `K?` when the graph is built instead. For `MapState` and broadcast keys, and for nullable components, the first release keeps the same rule.

Apart from the enum partition key, every rejection can be relaxed later without changing any existing key's bytes or hash code. Tightening a rule after users have keyed state by a type would strand that state, so the first release starts narrow.

### Deterministic bytes

The partition-key and `MapState` rules need equal keys to have equal bytes and unequal keys different bytes. They rely on these formats:

- Flink's own serializers for the built-ins, which `typeInfo` returns for them ([#7](https://github.com/Lychee-Technology/flinkt/issues/7)).
- #5's records, which write fields in a fixed order, so a record's bytes are deterministic when its fields' bytes are. Two properties of #5's format are therefore requirements. `Float` and `Double` go through `floatToIntBits` and `doubleToLongBits`, as `DataOutput.writeFloat` and `writeDouble` do, so every NaN has one encoding, matching data-class equality. Every `String` is encoded losslessly. `String.toByteArray(Charsets.UTF_8)` isn't lossless: it writes an unpaired surrogate such as `"\uD800"` as `?`, the same byte as `"?"` (checked on JDK 25), which would make two different keys one.
- #20's IDs, one fixed encoding per constant.
- #18's value-class representation, once #18 decides it.

Maps aren't deterministic ([Collections](#collections)), and no collection is a key.

### Where the rules apply

- **The view's `keyBy` with a reified key type** applies the partition-key rules to the key's full Kotlin type when the job graph is built, before it calls Flink.
- **The view's `keyBy` with an explicit `TypeInformation`** applies them if the `TypeInformation` is one of Flinkt's own classes, which carry the canonical Kotlin type. Any other `TypeInformation`, whether it comes from Flink, a library or the caller, is an intentional escape hatch. Flinkt checks nothing, Flink's `keyBy` and its own validation apply, and the caller gets Flink's guarantees instead of Flinkt's key-safety guarantee. It's the same exit as passing a `TypeInformation` for a type Flinkt doesn't model, or calling `asFlink().keyBy(…)`. `typeInfo<Long>()` returns Flink's `BasicTypeInfo` (#7), so it counts as foreign, and Flink accepts every built-in that Flinkt would. Policing a foreign `TypeInformation` would add little safety, because the caller can always use raw Flink.
- **`runtimeContext.mapState<K, V>()`** applies the `MapState` user-key rules when it binds the state in `open()`, before the first record.
- **`mapStateDescriptor<K, V>()`** applies no key rule. Its descriptor can become keyed `MapState` or broadcast state, and the helper can't tell which. It does create `K`'s serializer in the map-key role, which is true in both, so a later restore never migrates the key ([Schema evolution](schema-evolution.md#keys)). A descriptor passed to Flink directly gets Flink's checks.
- **Broadcast-state keys** have no enforcement point until a view offers broadcast. The README states their rules.

A rejection names the canonical Kotlin type, the context, the component that makes a composite ineligible, and the reason.

`isKeyType()` and `isSortKeyType()` return `false` for every Flinkt `TypeInformation`, and Flinkt supplies no `TypeComparator`. Flink defines a key type as hashable and comparable, and nothing in the first release compares Flinkt keys with a comparator. DataStream `keyBy` never reads `isKeyType()` (`KeyedStream.validateKeyType`), and BATCH sorting compares serialized bytes (`SortingDataInput`). The Flink paths that do read the flags are `sortPartition` by `KeySelector` and field-expression keys, on 1.20, 2.2 and 2.3 (checked in the jars). They reject a Flinkt key with Flink's own `InvalidProgramException` instead of looking for a comparator that doesn't exist. The [comparator contract](testing.md#comparator-and-key-semantics) applies if a later release adds comparators.

### What a snapshot can't see

A serializer snapshot compares schemas, not method bodies. Suppose a data class that keys persisted state gains an `override fun hashCode()`. Its schema is unchanged, but its key groups move. A changed `equals()` changes which keys are the same. Either is a breaking state change, even though the serialized schema is identical.

Flinkt doesn't try to detect this in the snapshot. The eligibility rules admit only compiler-generated equality over components whose hash codes are specified, so an eligible key's behavior follows from its schema. The same edit also makes the type ineligible, so the upgraded job's `keyBy` through the view fails when its graph is built. Through `asFlink()` or a foreign `TypeInformation`, nothing checks it. Cross-release savepoint tests ([#29](https://github.com/Lychee-Technology/flinkt/issues/29)) are the system-level backstop.

One dependency remains. The formula of the compiler-generated `hashCode()` is Kotlin compiler behavior, not a documented guarantee. For a data class it's `31 * h + component.hashCode()`, so `UserKey(1, 2)` hashes to 33. For a value class it's the underlying value's hash code. Both were checked on Kotlin 2.4.20. #32 pins the hash codes and key groups of representative keys as constants, so a Kotlin or JDK upgrade in Flinkt's build that changed them fails a test.

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

- **Binary layout.** The record layout, the null representation, the encoding of strings and other scalars, the snapshot format, and what `restoreSerializer()` restores ([#5](https://github.com/Lychee-Technology/flinkt/issues/5)). The decisions above constrain it:
  - the snapshot carries what [What the first snapshot must carry](schema-evolution.md#what-the-first-snapshot-must-carry) lists: a symbolic, recursive schema, separate snapshot and record format versions, one snapshot class name on every adapter, the JVM class name as type identity, fields identified by name in a canonical order, and the map-key role;
  - records carry no schema, and nested values are written inline, without a length prefix;
  - `restoreSerializer()` can return a serializer for the old layout, driven by the old schema, and returns nothing that reads the old bytes when the plan is incompatible. Whether it fails there or returns a serializer that refuses to read is still #5's to decide ([The old-layout serializer](schema-evolution.md#the-old-layout-serializer));
  - strings are lossless, unpaired surrogates included, and don't go through `DataOutput.writeUTF`, which rejects encodings over 65,535 bytes. If Flinkt adopts the char-varint format of Flink's `StringValue`, #5 specifies it as Flinkt's own format and tests it on every line, and generated code doesn't call Flink to write it;
  - each floating-point value has one encoding, so every NaN writes the same bytes;
  - nullable fields share a record-level null bitmap, which a record without nullable fields doesn't have. A top-level nullable type wraps the non-null type's serializer, Flink's built-ins included. #5 fixes the bitmap's size, bit order and field order, and the top-level marker.
- **Value classes.** When a value class may use its underlying type's serializer ([#18](https://github.com/Lychee-Technology/flinkt/issues/18)). [Schema evolution](#schema-evolution) and [Keys](#keys) already fix some of it. The direction is that `UserId(val value: Long)` writes exactly the bytes a `Long` writes, while its schema records the wrapper's identity and the underlying schema. So `Long`, `UserId` and `OrderId` stay three schemas with identical bytes, and switching between them, or changing the underlying type, is incompatible. A value class is a key only if its representation is deterministic.
- **Migration defaults.** Which added fields get a migration default, and how a declaration states one ([#37](https://github.com/Lychee-Technology/flinkt/issues/37)).

These are sequencing questions that don't affect the architecture:

- **Views after the first slice.** Which Flink types get views next: windows, `connect`, broadcast, joins. Which type-introducing methods of the existing views get hand-written versions, such as `getSideOutput`. The generator refuses these, so until then they're reached through `asFlink()`. A side output's element type comes from its `OutputTag`, not from the call. `OutputTag(id)` takes that type from Flink's `TypeExtractor` on 1.20 and 2.3, and `OutputTag(id, typeInfo<T>())` doesn't, so the design has to cover how tags are built.
- **Typed sources.** Whether typed-source helpers are worth adding, given that Flink's typed `fromSource` is `@Experimental`.
- **Overload selection.** Letting a view forward one overload of a name, which `broadcast()` needs.
- **KDoc for generated forwarders.** The first version has none. One option is to link each forwarder to the Flink method it calls, with Dokka external links to the Javadoc Flink publishes for each line. The other is to copy Flink's Javadoc text. Copying adds attribution obligations, because that text is under the Apache License 2.0 with a NOTICE file and Flinkt is MIT-licensed. A copy also describes the Java method's return type rather than the view's.
