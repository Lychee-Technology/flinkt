# Testing and CI

Flinkt sits across several compatibility boundaries:

```text
Kotlin source
    ↓
Kotlin overload resolution
    ↓
KSP-generated code
    ↓
Flink TypeInformation / serializers
    ↓
Flink runtime
    ↓
checkpoint / savepoint state
```

A test that proves one boundary does not prove the next one. For example:

- generated code compiling does not prove the intended `map {}` overload was selected;
- serializer round-trip does not prove compatibility with bytes written by a previous release;
- a job running on a new Flink version does not prove an old savepoint can be restored;
- `TypeSerializerSnapshot` returning `compatibleAsIs()` does not prove the new serializer can actually read old state.

The test strategy therefore treats compile-time behavior, serializer behavior, Flink integration, and persisted-state compatibility as separate contracts. The guiding rule is:

> Compile tests prove the Kotlin API, serializer fixtures prove the binary contract, and savepoint tests prove the system contract.

## What must remain true

Tests should protect a small set of invariants rather than mirror implementation structure. Flinkt is correct only if:

1. Kotlin calls on a view resolve to the intended Flinkt overload, and each result type comes from the call site.
2. Complete Kotlin type information reaches Flink where required.
3. Supported types do not silently fall back to generic or Kryo serialization.
4. Generated per-record code does not depend on Kotlin reflection.
5. Flinkt serializers satisfy Flink's serializer contracts.
6. Serializer snapshot compatibility results agree with real serialized bytes.
7. On every supported Flink adapter, the views make the same Flink calls as the code they stand for, and entering or leaving a view changes nothing Flink sees.
8. Persisted state can be restored across every upgrade path Flinkt claims to support.
9. Flinkt's public Kotlin API does not change accidentally.

## Test layers

The suite is layered:

```text
                real savepoint upgrade
                         ▲
                         │
                 Flink MiniCluster
                         ▲
                         │
              Flink adapter contract
                         ▲
                         │
           serializer compatibility
                         ▲
                         │
         type and serializer semantics
                         ▲
                         │
          Kotlin API compile tests
                         ▲
                         │
                  KSP tests
```

Lower layers should be fast and precise. Higher layers should be fewer, slower, and closer to production behavior. An end-to-end test should not replace a lower-level contract test merely because it exercises more code.

## KSP correctness

KSP tests must prove more than the presence of generated files. Given this declaration:

```kotlin
@FlinkType
data class User(
    val id: Long,
    val name: String,
)
```

the generated implementation should compile and use direct Kotlin/JVM operations equivalent to `value.id`, `value.name`, and `User(id = ..., name = ...)`.

The generated hot path must not depend on reflective operations such as:

```text
KProperty lookup
memberProperties
primaryConstructor.call(...)
componentN reflective lookup
java.lang.reflect.Field
```

The processor suite should exercise each type-system feature independently before combining them:

```text
primitive fields
String
nested generated types
nullable fields
collections
value classes
enums
sealed hierarchies
data objects
generic data classes
nested generics
```

A case such as `typeInfo<Envelope<List<User?>>>()` is particularly useful because several layers can accidentally erase it to a raw or `Any`-based representation.

### Compile failures are part of the API

Unsupported or ambiguous declarations need negative compilation tests. For example, this declaration should fail predictably if the current policy does not support `UnsupportedType`:

```kotlin
@FlinkType
data class Bad(
    val value: UnsupportedType,
)
```

The test should verify both the failure and the quality of the diagnostic. The error message should identify enough context to act on, such as the field `Bad.value` and the type `UnsupportedType`.

Negative cases should cover constraints such as duplicate stable IDs, invalid annotation combinations, unsupported generic forms, unsupported recursive structures, and unsupported fields. A processor that fails correctly but reports only an internal exception is still wrong.

## Incremental compilation

Code generation must remain correct under Gradle and KSP incremental builds, so a clean build is insufficient evidence. For example:

```text
User.kt   → User generated code
Order.kt  → Order generated code
```

Changing only `User.kt` should not unnecessarily regenerate independent per-type output for `Order`. A module-wide generated registry is different. It depends on every generated declaration in the module, so it is an aggregating output.

Functional Gradle tests should cover at least:

```text
clean build → edit declaration → incremental build
clean build → delete declaration → incremental build
clean build → rename declaration → incremental build
dependency module changes → consumer rebuild
```

These tests should use real Gradle/KSP builds rather than a mocked processor environment. Incremental behavior is part of correctness because stale generated metadata can be harder to diagnose than a failed clean build.

## Kotlin API compile contracts

Flinkt deliberately uses the same operator names as Flink:

```kotlin
stream.map { ... }
stream.filter { ... }
stream.keyBy { ... }
```

That makes Kotlin overload resolution part of the public contract. Tests must compile representative user code and assert the exact static type of each result. Runtime tests can't reliably catch this class of regression after the fact.

### Asserting an exact static type

A supertype assertion such as `expectType<DataStream<Event>>(x)` also accepts any subtype, so it can't show that `x` has exactly that type. Comparing `typeOf` of the inferred type with an expected `KType` doesn't work either: when a lambda converts to a Java function interface, Kotlin infers a flexible type such as `User!`, and its `KType` differs from `User`'s. Compile contracts use an invariant carrier, inferred from the value alone before any expected type applies:

```kotlin
class Exactly<T>

fun <T> exactTypeOf(value: T): Exactly<T> = Exactly()

fun <U> Exactly<U>.shouldBe() = Unit
```

```kotlin
val users =
    stream.map {
        User(it.id, it.name)
    }

exactTypeOf(users).shouldBe<FlinktSingleOutputStreamOperator<User>>()
exactTypeOf(users.keyBy { it.id }).shouldBe<FlinktKeyedStream<User, Long>>()
exactTypeOf(users.asFlink()).shouldBe<SingleOutputStreamOperator<User>>()
```

`exactTypeOf(x)` infers `Exactly<X>` from `x` alone, and `Exactly` is invariant, so `shouldBe<Y>()` compiles only when `X` is `Y`. A flexible `User!` still matches `User`. A subtype doesn't match: `exactTypeOf(subtypeValue).shouldBe<DataStream<String>>()` fails with a receiver type mismatch. That was checked against Kotlin 2.4.20.

### Operators take their result type from the call site

Each operator that introduces an element type must be covered in every form a user can call it:

```text
lambda                              map { User(...) }
Flink function literal              map(MapFunction<Event, User> { ... })
Kotlin function class               map(ToUser())
Java function class                 map(JavaToUser())
generic function class              process(Passthrough())
Kotlin nullable result              map { it.takeIf { ... } }       → R is nullable
explicit type argument              map<User?>(javaFunction)
```

Each case asserts the exact static type here, and the [type-information contracts](#typeinformation-contracts) check what Flink receives.

Two more tests complete this section:

- An operator called inside generic code where `R` isn't reified must not compile. The diagnostic is Kotlin's *cannot use 'R' as reified type parameter*, and the overload that takes a `TypeInformation` must compile in the same position.
- A structural test reads the public API of the view classes and fails when a method that introduces a type parameter in its result is neither `inline` with that parameter reified nor given a `TypeInformation` for it.

### Preserve the view through fluent calls

This chain must stay in the view:

```kotlin
stream
    .map { normalize(it) }
    .name("normalize")
    .uid("normalize-v1")
    .setParallelism(4)
    .filter { it.valid }
```

Compile contracts check the exact type after each fluent method. They run against every supported Flink adapter, because the views are compiled per line.

### Leaving the view is explicit

A view must never be a Flink stream. Otherwise inherited Flink methods would be callable on it, and a call could leave Flinkt without anything in the source showing it. Two tests protect this:

- A structural test fails if a view class extends or implements a Flink type.
- A negative compile test: `val d: DataStream<Event> = view` must not compile, and neither must a Flink method the view doesn't offer, such as `view.connect(other)`.

`.flinkt()` has a compile contract for each Flink type that can enter: `DataStream`, `SingleOutputStreamOperator` (including a `DataStreamSource`), and `KeyedStream`. Each one asserts the view type and asserts that `asFlink()` has the exact Flink type.

### Generated forwarders

Most view methods only forward, and they are [generated](architecture.md#generated-forwarders) from each line's Flink classes. The generator is where the view rules are enforced for those methods, so it needs its own evidence:

- **Each refusal is a test.** For each rule, list a method that breaks it in a test build and assert that the build fails with a message naming the method and the reason. The cases are:
  - a method missing on the line;
  - a type-introducing method (`map`, `connect`);
  - a method returning a type that has no view (`broadcast` with descriptors);
  - an `@Internal` method (`getTransformation`).

  These tests run on every adapter lane, because each line's classes differ. On 1.20 they also show that the `@Internal` check sees class-retention annotations.
- **Flink's stability status reaches Kotlin callers.** A Kotlin call to the forwarder of a deprecated Flink method must compile with a deprecation warning, as a Java call to Flink's method does. A call to the forwarder of an `@Experimental` method must warn without `@OptIn(ExperimentalFlinkApi::class)` and compile cleanly with it. An overload that only uses a deprecated type must be forwarded without an annotation.
- **Parameter names come from Flink's sources, exactly.** The extraction script fails unless every descriptor it writes matches the class files, both ways. Both JavaParser and KSP get Java varargs descriptors wrong, and JavaParser gets nested types wrong, so this check is what makes a lookup by descriptor trustworthy. The generator must fail the build when a forwarded method has no entry in the names file, and when the file records a different Flink version than the one on the classpath. The names file must be a tracked input of the generation task, so that a changed file regenerates the forwarders.
- **The generated API is reviewed like hand-written API.** Each adapter checks in the dump of its public API, generated members and their parameter names included, and the build fails when the dump doesn't match. When a Flink upgrade changes the overloads or names of a listed method, the change appears as a diff in the upgrade's pull request instead of shipping unnoticed.
- **Adapters agree.** A test compares the generated members of every adapter. Members for methods that exist on all lines must be identical, and anything else must be a listed per-line difference. On 1.20 today, those are the deprecated `partitionCustom`, `assignTimestampsAndWatermarks` and legacy `sinkTo` overloads.

Forwarded calls don't need a test each. The generator guarantees their shape: one call on `asFlink()`, and a returned stream wrapped in its view. The [adapter contract](#flink-adapter-contract) checks their behavior on a sample that includes `union`, a sink, and a fluent call after `forceNonParallel()`.

### State helpers

Descriptor helpers and binding helpers return different Flink types, and the tests pin both:

```kotlin
exactTypeOf(valueStateDescriptor<User>("user")).shouldBe<ValueStateDescriptor<User>>()
exactTypeOf(runtimeContext.valueState<User>("user")).shouldBe<ValueState<User>>()
```

The same applies to `list` and `map`. A negative compile test assigns `runtimeContext.valueState<User>("user")` to a `ValueStateDescriptor<User>`. A positive one compiles the README's `open()` example, where the type arguments come from the property type.

### Preserve access to native Flink APIs

Improving Kotlin ergonomics must not make established Flink APIs inaccessible:

- `asFlink()` must return the instance that entered, and tests assert identity.
- View operators must accept Flink's own function types, including `MapFunction`, `KeySelector`, `ProcessFunction`, `KeyedProcessFunction`, and their rich variants.

## TypeInformation contracts

`typeInfo<T>()` needs semantic tests independent of serializers. Representative cases include:

```kotlin
typeInfo<Int>()
typeInfo<String>()
typeInfo<User>()
typeInfo<List<User>>()
typeInfo<Map<Long, User>>()
typeInfo<User?>()
typeInfo<Envelope<User>>()
```

`typeInfo<String?>()` and `typeInfo<String>()` must differ, and so must the output types of `map { it.takeIf { ... } }` and `map { it }`. A platform type from Java code resolves as non-null, and a test pins that rule. A type Flinkt can't model must fail with an error that names the full Kotlin type, including its arguments and nullability.

Tests should inspect the semantics that matter to Flink:

```text
field names
field count
nested type information
generic arguments
nullability metadata where represented
serializer selection
composite-type behavior
```

For generated Kotlin types, and for any other type Flinkt documents as specialized, tests must prove the type is not silently handled by the generic fallback.

## Composite and field semantics

If a generated data class is exposed to Flink as a composite type, tests must cover field addressing rather than only serialization. A serializer can be completely correct while field-based keys or composite operations are wrong.

For these classes:

```kotlin
data class Address(
    val city: String,
)

data class User(
    val id: Long,
    val address: Address,
)
```

the type layer should be tested, where supported, with the field expressions `id`, `address`, `address.city`, and `*`. Relevant Flink operations include field lookup, flattened fields, and nested type lookup.

## Serializer contracts

Round-trip serialization is necessary but insufficient. Every Flinkt serializer should be exercised against the relevant Flink serializer contract, including behavior such as:

```text
create instance
serialize / deserialize
deserialize with reuse
copy
copy with reuse
stream-to-stream copy
duplicate
snapshot configuration
length reporting where applicable
```

The exact contract depends on the serializer type and Flink version.

Flink's own serializer test suites are useful models for what needs to be exercised, but Flinkt should not make its test architecture depend on unstable Flink test-source internals. Where Flink provides a reusable public or sufficiently stable test utility for a supported adapter, it may be used behind that adapter.

## Property-based serializer tests

Generated serializers handle combinations that are tedious to enumerate manually. Property-based testing is useful for:

```text
null combinations
boundary numeric values
empty strings
Unicode strings
empty collections
single-element collections
nested collections
nested nullable values
every sealed subtype
value-class boundary values
```

The core properties include:

```text
deserialize(serialize(x)) == x
copy(x) == x
```

and, where aliasing matters:

```text
copy(x) does not unexpectedly share mutable nested state
```

Property tests do not replace compatibility fixtures, because the current implementation normally both writes and reads the bytes.

## Binary format fixtures

For binary formats owned by Flinkt, selected records should have immutable golden representations. Examples might include:

```text
User v1
NullableUser v1
Envelope<User> v1
Event hierarchy v1
```

A golden test catches changes that ordinary round-trip tests would accept, such as:

```text
field order changed
null-mask layout changed
subtype tag changed
length encoding changed
```

A golden-byte failure means the persisted representation changed, which may be intended. Either way, the change needs an explicit compatibility decision.

## Reflection-free generated paths

"Reflection-free hot path" should be tested structurally rather than inferred from a benchmark. Use more than one signal.

Tests can inspect generated code or bytecode for forbidden references such as:

```text
kotlin/reflect
java/lang/reflect
primaryConstructor
memberProperties
```

A stronger integration test should execute the generated serialize, deserialize, copy, and field-access code without `kotlin-reflect` on the runtime classpath. If the supported generated path requires `kotlin-reflect` to be present, the zero-reflection claim is false regardless of benchmark results.

Performance profiling may provide additional evidence, but it is not the correctness test.

## Comparator and key semantics

If Flinkt owns comparator behavior, it needs a dedicated contract suite. Relevant behavior includes:

```text
object comparison
serialized comparison
hashing
key extraction
reference comparison
duplicate()
```

Object-level and serialized ordering must agree. For a composite key such as:

```kotlin
data class Key(
    val country: String,
    val id: Long,
)
```

tests should include equal and differing values at each key position. Comparator correctness should not be inferred from successful partitioning in one integration test.

## Flink adapter contract

Compile contracts show which static type a call returns. They can't show that the Flink calls behind it are the ones the user's code stands for. These tests run against each adapter's real Flink classes. They only need to build the stream graph, not run it, so no MiniCluster is required.

**Same graph as plain Flink.** Build a pipeline once through the views and once with the equivalent Flink calls. Pass the Flink calls, explicitly, the `TypeInformation` the view should produce, so that Flink's own type inference plays no part. The two stream graphs must match in:

- nodes and edges;
- partitioners;
- operator classes;
- output types;
- parallelism and max parallelism;
- UIDs and names.

A view operator that let Flink infer its type fails the comparison, and so does one that reached a different Flink method. Compare structure, not generated IDs, since those come from a global counter. Cover at least:

- the first-milestone chain;
- `process` on a keyed stream;
- `union`;
- a side output;
- a sink added with the view's `sinkTo`;
- `DataStreamUtils.reinterpretAsKeyedStream`, whose forward partitioning an extra keyed step would replace.

**Entering and leaving change nothing.** For each type that can enter, `x.flinkt().asFlink()` must be `x`, and the environment's transformations must be the same before and after entering.

**Flink still acts on per-object state.** Every field listed in [State on stream objects](flink-compatibility.md#state-on-stream-objects) for the adapter's line gets a case showing that a call through the view gives the same outcome as the call on the Flink object:

```text
forceNonParallel(), then setParallelism(2) or setMaxParallelism(2) through the view → rejected, as in Flink
side-output ID requested through the view, then with another type → rejected, as in Flink
non-parallel source, setParallelism(2) through the view           → rejected, as in Flink
2.x: enableAsyncState(), then process through the view            → asynchronous keyed operator, as in Flink
```

Views never copy these fields, so the cases pass by construction. They stay as regression tests against adapter code that builds its own Flink objects.

**No `@Internal` Flink methods.** The generator checks the methods it forwards. For the Flink methods that hand-written view code calls, such as `map(fn, TypeInformation)`, `keyBy`, `process(fn)` and `returns`, a test lists them and fails if one is annotated `@Internal` on the adapter's line. The test reads the annotations by reflection on 2.x and from bytecode on 1.20, where they have class retention. An `@Experimental` method must match a documented exception in the [dependency policy](flink-compatibility.md#dependency-policy-for-flink-apis).

These tests run on every adapter lane, because the Flink classes differ between lines.

## Flink runtime integration

MiniCluster tests establish that local components still work when assembled by a real Flink runtime. The integration pipeline should exercise the features that interact across boundaries, for example:

```text
source
  ↓
map producing generated data class
  ↓
filter
  ↓
keyBy using generated/value-class key
  ↓
keyed state
  ↓
process
  ↓
sink
```

The pipeline runs with Flink's `pipeline.generic-types` set to `false`, so any Kryo fallback anywhere in it fails the job. The test should verify both results and relevant type/runtime properties. This layer should stay focused. It should not reproduce the entire type matrix already covered by lower-level tests.

## State integration

State deserves dedicated runtime coverage because additional contracts appear after a serializer is placed behind a descriptor and state backend. Representative tests should cover `ValueState<T>`, `ListState<T>`, and `MapState<K, V>` for Flinkt-managed types.

A state test should exercise a lifecycle closer to production:

```text
write state
    ↓
checkpoint
    ↓
restart / restore
    ↓
continue processing
```

Direct serializer tests cannot prove that state descriptors, serializer creation, snapshots, classloading, and backend restore fit together correctly.

## Serializer snapshot compatibility

Every supported schema transition must have an explicit expected compatibility result. Examples include:

| Change | Expected result |
|---|---|
| identical schema | compatible as-is |
| field reorder | defined by serializer policy |
| append field | defined by serializer policy |
| insert field | defined by serializer policy |
| remove field | defined by serializer policy |
| rename field | defined by serializer policy |
| field type change | normally incompatible unless explicitly migrated |
| nullability change | defined by serializer policy |
| nested schema change | derived from nested compatibility |
| enum change | defined by stable-ID policy |
| sealed subtype change | defined by subtype-ID policy |
| value-class underlying type change | normally incompatible |

The table should be filled from the actual binary format, not from source-level intuition.

A build-time schema checker and `TypeSerializerSnapshot` answer different questions and should have separate tests.

### Compatibility results must be tested against real bytes

A snapshot result alone is not sufficient evidence. If `newSnapshot.resolveSchemaCompatibility(oldSnapshot)` returns `compatibleAsIs`, the test must also prove:

```text
old serializer
      ↓
old bytes
      ↓
new serializer
      ↓
correct value
```

If compatibility requires migration, the test must exercise that migration path.

This is a hard rule:

> Every serializer compatibility branch must have an old-bytes test.

Otherwise snapshot metadata and the actual wire format can drift apart unnoticed.

## Released serializer fixtures

Source code for old serializers will disappear as the project evolves, so compatibility tests need immutable artifacts created from released implementations. Conceptually:

```text
compatibility/
└── serializers/
    ├── 0.1/
    │   ├── user.bin
    │   ├── user.snapshot
    │   └── metadata.json
    └── 0.2/
        └── ...
```

Metadata should identify enough context to reproduce the contract, such as:

```text
Flinkt version
Flink version
Kotlin version where relevant
logical type
serializer format version
```

Released fixtures are historical evidence. A later implementation must not regenerate them merely to make tests pass.

## Savepoint compatibility

Serializer compatibility and savepoint compatibility are related but not equivalent. The highest-level compatibility test is:

```text
old Flinkt
+
old supported Flink
        │
        ▼
run job and create savepoint
        │
        ▼
new Flinkt
+
new supported Flink
        │
        ▼
restore savepoint
        │
        ▼
continue processing correctly
```

This proves the interaction among:

```text
operator identity
state descriptors
serializer snapshots
generated type discovery
classloading
Flink state backend
Flink version upgrade
```

A release must not claim an upgrade path is supported based only on serializer unit tests when savepoint restoration is part of that claim.

## Flink version compatibility

Every supported Flink minor line has its own adapter and test lane. The compatibility unit is the Flink minor line, such as `2.3.x` or `1.20.x`; [flink-compatibility.md](flink-compatibility.md#current-support-matrix) lists each line's status.

Within a supported line, CI should normally exercise both the first and the latest supported patch. This verifies that the adapter actually covers the range it claims.

Each adapter lane should run at least:

```text
Kotlin API compile contracts
Flink adapter contract tests
type-information contracts
serializer contracts
MiniCluster smoke tests
```

A successful `compileKotlin` is not sufficient to declare a Flink version supported. [Release criteria](#release-criteria) defines what is.

### Test upgrade edges, not every historical pair

Cross-version savepoint testing grows quickly if every old release is tested against every new release, so the matrix should focus on supported upgrade edges. Typical edges include:

```text
previous Flink minor → current Flink minor
previous Flinkt release → current Flinkt release
old Flinkt + same Flink line → new Flinkt + same Flink line
```

Adding a Flink minor line always declares one edge: from the previous minor line of the same Flink major, when Flinkt supports that line or adds it in the same release. State descriptors and serializer snapshot integration live in each adapter, so a same-version test can't show that the new adapter reads state written through the previous one. Other edges are declared per release.

A major-version edge should only exist where both Apache Flink and Flinkt explicitly support that migration.

## Flinkt API compatibility

Flinkt itself is a Kotlin library and needs an ABI contract independent of Flink compatibility. The build should detect unintended binary API changes in published modules, especially in signatures involving:

```text
inline functions
reified generics
default parameters
view methods
extension functions
```

ABI validation catches JVM-level changes but not every Kotlin source-resolution regression, which is why compile fixtures remain necessary.

### Source compatibility fixtures

Keep a small suite of representative user programs that are compiled against every candidate release. Examples should cover:

```text
map returning generated type
generic map result
keyBy with value class
explicit Flink MapFunction
generic state
fluent operator chain
entering a view and leaving it with asFlink()
```

These fixtures catch problems that binary API comparison may not detect, such as:

```text
previously valid call becomes ambiguous
different overload is selected
type inference regresses
```

## Multi-module behavior

Generated types must be tested in the project shape users are likely to have:

```text
:model
  @FlinkType User

:job
  DataStream<User>
```

The producer module should generate and publish its metadata. The consumer should reuse that metadata without regenerating the same type.

Tests should cover dependency changes and classpath discovery, including cases where KSP is enabled only in the module that owns the annotated declaration. Single-module processor tests do not prove this behavior.

## Classloader behavior

Flink jobs do not always run with the same classloader arrangement as ordinary Gradle unit tests. Tests should therefore exercise generated module discovery and serializer snapshot restore under a user-code classloader setup representative of Flink. Particular risks include:

```text
generated registry discovery
model-class resolution
serializer snapshot restoration
duplicate runtime classes
```

A test that passes only under the test runner's system classloader is not sufficient evidence.

## Table integration

If Flinkt provides Table API support, it needs a separate contract suite. DataStream `TypeInformation` tests do not prove Table logical-type behavior.

For example, this class:

```kotlin
data class User(
    val id: Long,
    val name: String?,
)
```

should be tested for semantics equivalent to:

```text
id   BIGINT NOT NULL
name STRING
```

Useful cases include:

```text
nullable/non-null primitives
data class → ROW
nested data class → nested ROW
List<T> → ARRAY
Map<K,V> → MAP
```

Integration coverage should include actual Table/DataStream conversion rather than only inspecting the generated `DataType`.

## Performance tests

Performance measurements are separate from correctness. Do not put timing assertions such as "serialization must finish in less than N milliseconds" into ordinary unit tests.

Use a JVM benchmark harness and measure operations such as:

```text
serializer creation
serialize
deserialize
copy
key extraction
comparison
```

Useful comparisons include:

```text
Flinkt generated path
reflection-based implementation
relevant Flink baseline
```

The important metrics are normally:

```text
throughput
latency per operation
allocations per operation
```

Benchmarks should help detect regressions and validate design choices. They should not turn normal PR CI into a noisy timing gate.

## CI tiers

Different evidence belongs at different frequencies.

### Pull requests

PR CI should optimize for precise failures and reasonable latency. Run:

```text
unit tests
KSP compilation tests
negative compilation tests
API resolution tests
Flink adapter contract tests
type-information contracts
serializer contracts
snapshot compatibility logic
ABI validation
incremental KSP smoke test
MiniCluster smoke test
```

These run on the **primary adapter**, which is the adapter for the newest Flink minor line the community supports. Today that's 2.3. The rule names a line rather than a fixed version, so this document doesn't change each time Flink releases a minor version. The first slice is built on that adapter.

PR CI also compiles the shared adapter source against Flink 1.20 and runs its compile contracts and adapter contract tests there, without a MiniCluster. 1.20 is where Flink's type protocol differs most from 2.x ([Differences between target lines](flink-compatibility.md#differences-between-target-lines)), and catching a 1.x/2.x difference on the PR that introduces it costs a compile and a graph build. Other lines join merge and nightly CI once their adapters exist.

A PR should not need to restore every historical savepoint.

### Merge and nightly

Merge and nightly CI broaden version and environment coverage:

```text
all supported Flink adapters
first/latest patch of each supported Flink line
multi-module Gradle tests
classloader tests
larger property-based runs
full MiniCluster integration suite
Table integration tests
benchmark smoke runs
```

Failures in these lanes should be treated as compatibility regressions even if the primary PR lane remains green.

### Release

Release CI provides the evidence behind compatibility claims. It should include:

```text
every supported and candidate Flink adapter lane
state integration suite on each of those adapters
all immutable serializer fixtures
previous released Flinkt → candidate Flinkt compatibility
real savepoint restoration
supported Flink upgrade edges
published-artifact consumer tests
runtime Flink-version mismatch guard
```

[Release criteria](#release-criteria) defines what this suite must show before a Flink line is marked **Supported target**.

## Suggested repository structure

The physical layout can vary, but compatibility history should remain visibly separate from ordinary tests. Ordinary tests describe the current implementation; `compatibility/` records history. One useful organization is:

```text
flinkt/
├── flinkt-core/
│   └── src/test/
├── flinkt-ksp/
│   └── src/test/
├── flinkt-flinkXX/
│   └── src/test/
├── flinkt-testkit/
├── integration-tests/
│   ├── api-resolution/
│   ├── gradle/
│   ├── multi-module/
│   ├── minicluster/
│   ├── classloader/
│   └── table/
├── compatibility/
│   ├── serializers/
│   └── savepoints/
└── benchmarks/
```

## Testkit

A dedicated `flinkt-testkit` can centralize recurring contract assertions without hiding Flink semantics. Potential helpers include:

```kotlin
serializerContract(...)
typeInformationContract(...)
snapshotCompatibility(...)
```

The expected Flink compatibility result should remain explicit in the test:

```text
compatible as-is
compatible after migration
incompatible
```

A helper that turns all compatibility checks into a generic "passes" assertion would hide which of these results the test expects.

## Review focus

Testing changes deserve review at the boundaries where false confidence is easiest.

**Compile tests:** Does the test prove which Kotlin overload is selected, or merely that some overload compiles? Where the design promises a compile error, does a negative compile test check it, or only a runtime rejection?

**View behavior:** Would the test fail if a view method made a different Flink call than the code it stands for, or if entering a view built or changed a Flink object? Does an exact-type assertion actually reject a subtype?

**Type fallback:** Would the test fail if a generated type silently became generic/Kryo-serialized?

**Serializer compatibility:** Does the test read bytes created by the old implementation, or are both writer and reader the new serializer?

**Snapshot results:** Is `compatibleAsIs` backed by a real old-bytes test?

**Savepoints:** Is a claimed Flink/Flinkt upgrade path exercised through a real savepoint restore?

**Version matrix:** Does every advertised Flink minor adapter run against the patch range it claims?

**Historical fixtures:** Can release fixtures be accidentally regenerated or rewritten by current tests?

**Generated hot path:** Does the reflection test inspect behavior strongly enough to catch indirect runtime reflection?

## Release criteria

This section is the only definition of when a Flink line may be marked **Supported target** in the [support matrix](flink-compatibility.md#current-support-matrix). Other documents link here instead of restating it.

A Flinkt release should not describe a combination as supported solely because it compiles. A Flink line becomes **Supported target** only when the [release-tier suite](#release) passes with that line's adapter included, and the results show that, on that adapter:

```text
the Kotlin API resolves correctly, with result types from the call site
the views survive normal Flink chaining
the views build the same stream graph as direct Flink calls, and Flink still acts on per-object state
supported types preserve their intended type information
generated serializers satisfy their contracts
no documented specialized type silently falls back to generic serialization
the MiniCluster integration pipeline produces correct results
state survives checkpoint, restart, and restore, and processing continues correctly
serializer compatibility claims match old bytes
every declared upgrade edge into the line restores a real savepoint
```

Savepoint upgrade tests belong to upgrade edges, not to adapters, because an edge needs an older combination to restore from. [Test upgrade edges, not every historical pair](#test-upgrade-edges-not-every-historical-pair) says which edges a new minor line must declare. The first Flinkt release has no older Flinkt to restore from, and a line without a supported predecessor, such as `1.20.x` today, has no cross-line edge. Where a line has no edge to test, its persisted-state evidence is the same-adapter checkpoint restore and the serializer fixtures.

The exact CI implementation may evolve, but these proofs should remain separate so that a failure shows which contract broke.
