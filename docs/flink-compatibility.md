# Apache Flink Version Compatibility

Flinkt versions independently from Apache Flink.

A Flinkt version number describes Flinkt's Kotlin API, type system, KSP model, and generated-code contract. It does not imply compatibility with every Apache Flink release carrying the same major version. Each Flink-facing binary targets one Apache Flink minor release line.

For example:

```text
Flinkt 0.4.x
├── Apache Flink 2.3.x adapter
├── Apache Flink 2.2.x adapter
└── Apache Flink 1.20.x adapter
```

The application API should remain the same across these adapters:

```kotlin
stream
    .flinkt()
    .map { transform(it) }
    .filter { it.valid }
    .keyBy { it.id }
```

Changing the Apache Flink version should normally mean swapping the Flink integration artifact and leaving application code alone.

## Policy summary

1. Flinkt versions independently from Apache Flink.
2. A Flink adapter targets one supported Flink minor release line.
3. Only Apache Flink community-supported release lines receive active Flinkt support.
4. Flink API differences stay behind version-specific integration boundaries.
5. API compatibility and persisted-state compatibility are tested separately.

## Support policy

Flinkt supports only the Apache Flink release lines that the Apache Flink community still supports.

Cloud-provider availability does not extend Flinkt support. For example, Amazon Managed Service for Apache Flink may keep running releases that the Apache Flink community no longer supports. Those releases are outside the Flinkt support policy even while they remain available on AWS.

The AWS Managed Service for Apache Flink version table is a useful secondary reference because it reports both AWS support status and Apache Flink community support status. When the two disagree, the Apache Flink project is the source of truth.

Being downloadable from the Apache archive doesn't make a release supported either.

### Current support matrix

Status as of September 27, 2026. Community status comes from the Apache Flink [update policy](https://flink.apache.org/downloads/#update-policy-for-old-releases), which supports the current and previous minor release, and from the [Flink 1.20 LTS](https://flink.apache.org/documentation/flink-lts/) designation.

| Apache Flink line | Community status | Flinkt status | Notes |
|---|---|---|---|
| `2.3.x` | Supported | Not yet supported | Target line; no adapter yet |
| `2.2.x` | Supported | Not yet supported | Target line; no adapter yet |
| `1.20.x` | Supported (1.x LTS) | Not yet supported | Target line; no adapter yet |
| `2.1.x` | Ended | Not supported | Lost support when 2.3.0 was released on June 25, 2026 |
| `2.0.x` | Ended | Not supported | |
| `1.19.x` and older | Ended | Not supported | |

A target line becomes **Supported target** only after its adapter meets the [release criteria](testing.md#release-criteria) in the test strategy. No adapter exists yet, so Flinkt doesn't claim support for any line.

The table is a snapshot. The project should update it whenever Apache Flink changes its supported release lines. [Dropping Flink support](#dropping-flink-support) covers what happens to a line that leaves community support.

## Compatibility unit: the Flink minor line

Flinkt treats an Apache Flink minor release line as the normal compatibility unit. Patch releases such as `2.3.0`, `2.3.1`, and `2.3.2` belong to the `2.3.x` line and should normally use the same Flinkt adapter. A new minor line such as `2.4.x` is a new compatibility target.

The unit is the minor line because Apache Flink's API stability guarantees differ between patch, minor, and major upgrades. Flinkt ships compiled JVM bytecode, so source compatibility alone doesn't show that an existing Flinkt binary is safe on a new Flink minor release. Flinkt therefore recompiles and tests every newly supported Flink minor line explicitly.

## Versioning model

Flinkt's semantic version is independent of Apache Flink's. Don't encode the Flink version in the Flinkt project version: Flinkt 2.3.x should not mean Flink 2.3.x. One Flinkt release can support several Flink lines.

This keeps two different kinds of compatibility separate:

```text
Flinkt semantic version
        │
        └── public Kotlin API / generated-code contract

Apache Flink version
        │
        └── Flink JVM API / ABI compatibility
```

A change in either one can require a release without pretending that the other changed with it.

## Architecture boundary

Flink-dependent code must stay separate from Flinkt's Kotlin type model.

Conceptually:

```text
                    Flinkt core
                         │
              ┌──────────┴──────────┐
              │                     │
        Kotlin type model      generated codec SPI
              │                     │
              └──────────┬──────────┘
                         │
                  stable boundary
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   Flink 2.3         Flink 2.2        Flink 1.20
    adapter            adapter           adapter
        │                │                │
        ▼                ▼                ▼
 Apache Flink        Apache Flink      Apache Flink
    2.3.x               2.2.x             1.20.x
```

The boundary keeps version-sensitive Flink APIs out of components that don't need to change when Flink changes. It does not abstract Apache Flink away.

### Version-independent modules

These have no dependency on Flink:

```text
flinkt-core    @FlinkType, schema and type model, generated-codec SPI
flinkt-ksp     build time only
generated codecs and schema metadata
```

These layers describe Kotlin types and generated access patterns. They should not need to change because a method was added to `DataStream` or because a Flink serializer configuration API changed. [Layers and modules](architecture.md#layers-and-modules) gives the reason for each module.

### Version-specific integration

Each Flink adapter owns the APIs whose compatibility depends directly on Apache Flink:

```text
views: FlinktDataStream, FlinktSingleOutputStreamOperator, FlinktKeyedStream
typeInfo<T>() and TypeInformation integration
TypeSerializer and TypeSerializerSnapshot for generated types
state helpers
Table API integration
Flink runtime version checks
```

A Flink minor upgrade should therefore look mostly like this:

```text
new Flink release
      │
      ▼
new / updated Flink adapter
      │
      ▼
compile and compatibility tests
```

The annotations, the Kotlin schema, the KSP model, and the application API should come through unchanged. If ordinary Flink minor upgrades repeatedly require changes above the adapter boundary, that boundary should be reconsidered.

## Views and version compatibility

Flinkt's views hold Flink stream objects rather than extend Flink classes ([decision](architecture.md#decision-streams-are-views-not-flink-subtypes)). Each view method calls one Flink method on the object it holds, so a view depends only on the Flink methods it calls:

- **Flink adds a method.** Existing view calls don't change. Users reach the new method through `asFlink()` until a view offers it. If Flink adds an overload to a method a view [forwards](architecture.md#generated-forwarders), the generator forwards it as well. The adapter's checked-in API dump shows that change in review.
- **Flink removes a method a view calls or forwards, or changes its signature.** Hand-written view code then fails to compile against that line. For a forwarded method, generation fails if the method is gone or its new signature breaks a [view rule](architecture.md#generated-forwarders). Any other signature change is forwarded as Flink declares it, and the cross-adapter comparison fails until the difference is listed. Each case is caught before release and doesn't reach a user's job.
- **Flink changes a method's stability status.** A forwarder carries the status Flink declares, so only a change to `@Internal` fails generation. A method that becomes `@Experimental` is forwarded behind the `@ExperimentalFlinkApi` opt-in, and one that becomes deprecated is forwarded with Kotlin's `@Deprecated`, so Kotlin callers get a warning. A passing generation step therefore says nothing about status. The cross-adapter comparison fails until the new status is listed as a per-line difference ([Drift](architecture.md#drift)), and the review of a new line starts from a diff of its stability annotations ([Adopting a new Flink minor release](#adopting-a-new-flink-minor-release)). For a Flink method that hand-written view code calls, the adapter contract test fails if the method becomes `@Internal`, or becomes `@Experimental` without a listed [exception](#experimental).
- **Flink moves state between a stream object and its transformation.** Views don't copy either, so nothing needs updating. The adapter contract tests still check the known cases per line.

The adapters for all target lines are compiled from one shared source set. A per-line source set holds only what differs between lines.

### Differences between target lines

Checked by compiling and running the same probes against Flink 1.20.5, 2.2.1, and 2.3.0 ([spike](https://github.com/Lychee-Technology/flinkt/tree/4cccdc5ec6438d2f76e6598b0373fd2b1f563471/spikes/architecture-reset)). 2.2 and 2.3 have identical signatures on `DataStream`, `SingleOutputStreamOperator`, `KeyedStream`, `TypeInformation`, `TypeSerializer`, and `TypeSerializerSnapshot`.

| Area | 1.20 | 2.2, 2.3 | Effect on the adapter |
|---|---|---|---|
| `TypeInformation` serializer factory | abstract `createSerializer(ExecutionConfig)`; `createSerializer(SerializerConfig)` has a default | abstract `createSerializer(SerializerConfig)` only | per-line base class for Flinkt's `TypeInformation` |
| `TypeSerializerSnapshot.resolveSchemaCompatibility(snapshot)` | default method | abstract | implemented in both; no per-line code |
| `enableAsyncState()` | absent | on `KeyedStream` and `SingleOutputStreamOperator`, `@Experimental` | 2.x source set only, if a view offers it |
| Deprecated stream API | `keyBy(int...)`, `keyBy(String...)`, `timeWindow*`, `iterate`, `SinkFunction` sinks, `partitionCustom` by field, `assignTimestampsAndWatermarks` with the old assigners, `sinkTo` for the legacy `connector.sink.Sink` | removed; legacy `SourceFunction`/`SinkFunction` moved to `...legacy` packages | the overloads of forwarded methods (`partitionCustom`, `assignTimestampsAndWatermarks`, `sinkTo`) are forwarded on 1.20 as Flink declares them, which is a listed per-line difference; the rest aren't view methods |

### State on stream objects

Flink keeps some configuration on stream objects rather than on their `Transformation`. A second object built over the same transformation doesn't see it, and that is why Flinkt never builds one.

| Class | Field | Visibility | Set by | Read by | Lines |
|---|---|---|---|---|---|
| `SingleOutputStreamOperator` | `nonParallel` | protected | `forceNonParallel()`, which Flink also calls on `windowAll` results | `setParallelism`, `setMaxParallelism` | 1.20, 2.2, 2.3 |
| `SingleOutputStreamOperator` | `requestedSideOutputs` | private | `getSideOutput` | `getSideOutput`, which rejects a known ID with a different type | 1.20, 2.2, 2.3 |
| `KeyedStream` | `keySelector`, `keyType` | private, with public getters | constructor | keyed operators | 1.20, 2.2, 2.3 |
| `KeyedStream` | `isEnableAsyncState` | private | `enableAsyncState()` | keyed operators | 2.2, 2.3 |
| `DataStreamSource` | `isParallel` | private | constructor | `setParallelism` | 1.20, 2.2, 2.3 |

A `KeyedStream`'s `PartitionTransformation` can't be shared either: its public constructors always create a new one. The [adapter contract tests](testing.md#flink-adapter-contract) cover each field listed for a line. Adopting a new line includes checking this table against its sources.

## Preserve fluent chains

The view must remain active through normal Flink configuration:

```kotlin
stream
    .map { normalize(it) }
    .name("normalize")
    .uid("normalize-v1")
    .setParallelism(8)
    .filter { it.valid }
```

A fluent method on a view calls the Flink method and returns a view of what Flink returned. When a Flink upgrade changes one of these methods, only the adapter changes. A fluent method the view doesn't offer yet is a compile error on the view and is available through `asFlink()`.

## Keep generated code independent where possible

KSP-generated code should not bind itself to a specific Flink ABI. The separation is:

```text
User Kotlin source
        │
        ▼
       KSP
        │
        ▼
User generated codec          depends on flinkt-core and the JDK only
        │
        │ GeneratedCodec SPI  (java.io.DataInput / DataOutput)
        ▼
Flinkt TypeInformation,       one set per adapter, shared by every generated type
TypeSerializer, snapshot
        │
        ▼
Apache Flink
```

For example, generated code can read `User`'s fields and call its constructor directly, without owning any of Flink's `TypeSerializerSnapshot` compatibility logic:

```kotlin
value.id
value.name

User(
    id = ...,
    name = ...,
)
```

This separation is required rather than just tidy. `TypeInformation`'s abstract methods differ between 1.20 and 2.x (see the table above), so a generated `TypeInformation` subclass would need a separate build for each Flink line. Flink's `DataInputView` and `DataOutputView` implement `DataInput` and `DataOutput`, so the adapter passes them to generated code without copying.

The main reason for code generation is to remove reflection from the record hot path. This separation keeps that job apart from Flink-version-specific runtime contracts.

## Dependency policy for Flink APIs

Apache Flink's API stability annotations decide where, and whether, Flinkt may depend on a Flink API.

### `@Public`

`@Public` APIs are the preferred integration surface. Code that uses them is still recompiled and tested against every supported Flink minor line, because Flinkt ships binary artifacts and source compatibility is not binary compatibility.

### `@PublicEvolving`

`@PublicEvolving` APIs may change between Flink minor releases, so dependencies on them should stay inside the version-specific integration layer. A change in such an API should require only an adapter update, unless the Flinkt contract itself needs to change.

### `@Experimental`

Experimental Flink APIs should not be part of Flinkt's stable public contract. If Flinkt exposes functionality built on an experimental Flink API, that functionality should also be explicitly experimental.

Views follow this rule mechanically. When a view forwards an `@Experimental` Flink method, such as `enableAsyncState()` in the 2.x source set, the generated member carries `@ExperimentalFlinkApi`. That's a Kotlin opt-in marker at warning level, so a call without `@OptIn` warns ([Generated forwarders](architecture.md#generated-forwarders)). `StreamExecutionEnvironment.fromSource(…, TypeInformation)` is `@Experimental` on every target line, and it's the reason Flinkt has no environment view ([Views](architecture.md#no-environment-view)).

Hand-written view code in the first slice calls no `@Experimental` Flink method: `map(fn, TypeInformation)`, `keyBy(selector, TypeInformation)`, `process(fn)` and `returns(TypeInformation)` are `@Public` or `@PublicEvolving` on every target line. A hand-written call to an `@Experimental` method is an exception. It's listed here with its reason, and the view method that makes the call carries `@ExperimentalFlinkApi` too. The [adapter contract test](testing.md#flink-adapter-contract) fails on an `@Experimental` call that isn't listed. No call is listed today.

### Deprecated

A view forwards a deprecated Flink method, and the generated member keeps it deprecated with Kotlin's `@Deprecated`. A Kotlin caller gets the warning that a Java caller of Flink's method gets. Flinkt doesn't remove what Flink still ships.

### `@Internal`

Depending on Flink `@Internal` APIs is discouraged. If an internal API is unavoidable, the dependency must:

1. remain inside a version-specific adapter;
2. have a documented reason;
3. be covered by compatibility tests;
4. be replaceable without changing Flinkt's public Kotlin API.

An internal Flink API must never become part of Flinkt's own public ABI.

Watch for `@Internal` on individual methods of public classes. On `DataStream` and `KeyedStream`, `process(fn, TypeInformation)` is `@Internal` in 1.20, 2.2, and 2.3, while the one-argument `process(fn)` and `returns(TypeInformation)` are public. Views therefore type `process` with `process(fn).returns(typeInfo<R>())`. The adapter currently needs no `@Internal` API, and a [test](testing.md#flink-adapter-contract) checks every Flink method it calls. Flink 1.20's stability annotations have class retention, so that test reads them from bytecode.

## Do not build correctness around `TypeExtractor`

Flinkt should not rely on Flink reconstructing complete Kotlin types from generated JVM bytecode.

The preferred path is:

```text
Kotlin compiler / KSP knows T
            │
            ▼
        typeInfo<T>()
            │
            ▼
explicit TypeInformation<T>
            │
            ▼
        Apache Flink API
```

rather than:

```text
Kotlin lambda
     │
     ▼
compiled JVM generic metadata
     │
     ▼
Flink TypeExtractor
     │
     ▼
hope the complete Kotlin type survived
```

This is especially important for:

```text
nullable types
nested generics
value classes
sealed hierarchies
generated data-class serializers
```

Flink type extraction can still be used where appropriate, but it is not the foundation of Flinkt's Kotlin type system.

`TypeExtractor` still runs inside some Flink methods that views call. Flink's one-argument `process(fn)` infers a type, which is `GenericType` for a Kotlin data class, and the view replaces it with `returns()` before anything reads it. The [adapter contract](testing.md#flink-adapter-contract) checks the final output type, not the path to it.

## Dependency packaging

Flinkt must not package a private Apache Flink runtime inside its normal library artifacts. The deployment environment owns the Flink runtime version, so Flink dependencies should use the equivalent of `compileOnly` / `provided` semantics where appropriate.

The intended relationship is:

```text
Flink cluster / application
       │
       └── chooses Apache Flink version

Flinkt
       │
       └── supplies matching integration code
```

Bundling conflicting Flink runtime classes, such as a job jar that embeds a Flink 2.2 runtime and runs on a Flink 2.3 cluster, causes classloader and binary compatibility failures. Flinkt should avoid them by design.

## Runtime version guard

A version-specific adapter should fail early when loaded against an unsupported Flink minor line. For example:

```text
Flinkt adapter mismatch

This artifact supports Apache Flink 2.3.x,
but the runtime reports Apache Flink 2.4.0.

Use the Flinkt adapter for Flink 2.4.x.
```

A deliberate version error is preferable to letting execution continue until it fails with errors such as:

```text
NoSuchMethodError
AbstractMethodError
ClassNotFoundException
```

The guard is a diagnostic measure. It does not replace compile-time and integration testing.

## Adopting a new Flink minor release

A new Apache Flink minor line is handled as an adapter upgrade. For example:

```text
Apache Flink 2.4.0 released
          │
          ▼
confirm community support status
          │
          ▼
create/update Flink 2.4 adapter
          │
          ▼
compile against Flink 2.4
          │
          ▼
resolve API differences
          │
          ▼
pass the release-tier suite on the 2.4 lane
          │
          ▼
mark 2.4.x Supported target in the matrix
```

The process should not assume compatibility merely because existing code compiles. Adopting a line starts by extracting its `flink-api.tsv` from its class files ([Parameter names](architecture.md#parameter-names)). Building the adapter then regenerates the view forwarders from the new line's classes. The diffs of the names file and of the adapter's API dump show what changed. The review starts from a diff of the new line's API surface against the previous line: public methods, their stability annotations, and the fields of the stream classes. The [spike inventory](https://github.com/Lychee-Technology/flinkt/tree/4cccdc5ec6438d2f76e6598b0373fd2b1f563471/spikes/architecture-reset/inventory) is a working example. Particular attention should go to:

```text
Flink methods the views call: existence, signature, stability annotation
state kept on stream objects
TypeInformation
TypeSerializer
TypeSerializerSnapshot
SerializerConfig
state descriptors
Table DataType APIs
```

Flinkt does not promise same-day support for a new minor line. The line becomes **Supported target** when its adapter meets the [release criteria](testing.md#release-criteria), which require runtime and persisted-state evidence as well as compile and unit-level tests, and the support matrix is updated. Until then, the line is "Not yet supported", even though it shares a major version with a supported line.

## Patch upgrades

Patch upgrades within a supported minor line should not normally require another public Flinkt artifact family. Moving from `2.3.0` to `2.3.1`, for example, keeps the Flink `2.3.x` adapter.

CI should still validate the supported patch range. At minimum, tests should cover the first and the latest supported patch of each minor line. When Flink publishes a new patch release, Flinkt should add it to CI before claiming it as tested.

## Major upgrades

A Flink major release, such as the move from 1.x to 2.x, is a migration boundary. It may remove or redesign APIs on purpose.

Flinkt should expect the following layers to need independent adaptation:

```text
views
TypeInformation bindings
serializer runtime bindings
state APIs
Table integration
```

The following should preferably survive unchanged:

```text
@FlinkType annotations
Kotlin schema model
generated field access
generated constructors
generated codec contract
```

A major-version adapter may therefore contain substantially different Flink-facing code while presenting the same Flinkt application API where the underlying Flink semantics still allow it.

## CI compatibility matrix

Every target minor line gets its own build and integration lane:

| Adapter | Minimum tested Flink | Latest tested Flink |
|---|---:|---:|
| Flink `2.3.x` | `2.3.0` | latest `2.3.x` |
| Flink `2.2.x` | `2.2.0` | latest `2.2.x` |
| Flink `1.20.x` | `1.20.0` | latest `1.20.x` |

A successful compile is not enough. The compatibility suite should exercise the contracts Flinkt adds on top of Flink:

```text
entering and leaving views
map
filter
keyBy
process
name
uid
parallelism configuration
state helpers
typeInfo<T>()
generated serializers
generated type registry
```

The suite should verify both overload resolution and the actual type information visible to Flink. Which tests run on which lane, and in which CI tier, is defined in [testing.md](testing.md#ci-tiers).

## API compatibility and state compatibility are different

Two independent questions must be answered when upgrading:

```text
1. Does Flinkt compile and run against the new Flink version?

2. Can the new Flinkt + Flink combination restore state
   written by the previous combination?
```

The first is JVM/API compatibility and the second is persisted-state compatibility. Passing the first does not prove the second, so serializer changes need dedicated compatibility tests that use state written by an older implementation.

## State compatibility fixtures

Flinkt should maintain immutable compatibility fixtures for released serializer formats. For example:

```text
compatibility/
├── user-v1/
│   ├── serializer-snapshot
│   └── serialized-state
├── nullable-user-v1/
│   ├── serializer-snapshot
│   └── serialized-state
└── sealed-event-v1/
    ├── serializer-snapshot
    └── serialized-state
```

Tests should follow the direction that matters in production:

```text
old Flinkt + old Flink
          │
          ▼
       write state
          │
          ▼
  immutable fixture
          │
          ▼
new Flinkt + new Flink
          │
          ▼
       restore
```

A round trip that writes and reads with only the newest serializer does not provide the same evidence.

Persisted serializer formats deserve stricter compatibility review than ordinary API wrappers, because users' checkpoints and savepoints hold data in those formats and that data has to restore across application and Flink upgrades.

## Dropping Flink support

Flinkt follows Apache Flink community support rather than maintaining its own long-lived fork of old Flink APIs. That keeps Flinkt's maintenance on the versions the Flink community still maintains.

When the community ends support for a minor line:

1. the existing Flinkt adapter remains published;
2. documentation marks the line as unsupported;
3. new Flinkt releases are not required to provide an adapter for it;
4. security or compatibility fixes are not normally backported;
5. users should upgrade Flink before expecting support from newer Flinkt versions.

## Artifact model

The exact Maven coordinates are a packaging decision, but the dependency model should expose the Flink compatibility choice clearly. For example:

```kotlin
dependencies {
    implementation(
        platform("io.flinkt:flinkt-bom:<flinkt-version>")
    )

    implementation(
        "io.flinkt:flinkt-flink23"
    )

    ksp(
        "io.flinkt:flinkt-ksp"
    )
}
```

An application moving from Flink `2.3.x` to `2.4.x` should conceptually change its Flinkt artifact along with its own Flink runtime dependency:

```diff
- implementation("io.flinkt:flinkt-flink23")
+ implementation("io.flinkt:flinkt-flink24")
```

Its application code should normally stay the same. The artifact name may expose Flink compatibility; the application API should not.

## Review rules

Review changes that involve the Apache Flink integration against four separate concerns.

### Flink API dependency

Is the code using `@Public`, `@PublicEvolving`, `@Experimental`, or `@Internal` Flink APIs? Could the dependency move behind the version adapter?

### Public Flinkt API

Does adapting to a new Flink version unnecessarily change normal application code? If so, determine whether changed Flink semantics caused the difference or adapter implementation detail leaked into the API.

### Generated-code contract

Does the Flink upgrade force KSP-generated application code to change? If so, determine whether the generated code depends on more Flink ABI than necessary.

### Persisted state

Can the new serializer and snapshot actually restore old state? Base the answer on compatibility fixtures and serializer behavior. Matching schema metadata alone doesn't prove it.
