# Flinkt

**Kotlin-first APIs and compile-time type support for Apache Flink.**

Flinkt is a Kotlin-focused integration layer for Apache Flink. It keeps Flink's programming model and operator vocabulary, while addressing the places where Java-oriented type extraction and APIs do not map cleanly to Kotlin.

Application code should look like ordinary Flink code:

```kotlin
val events =
    env.fromSource(
        source,
        watermarkStrategy,
        "events",
    )

val sessions =
    events
        .filter { it.payload.isNotBlank() }
        .map {
            UserEvent(
                userId = it.userId,
                payload = it.payload.trim(),
            )
        }
        .name("normalize")
        .uid("normalize-v1")
        .keyBy { it.userId }
        .process(SessionFunction())
```

## Why

Kotlin code often contains more type information than Flink can reliably recover through Java reflection.

This becomes visible with types such as:

```kotlin
data class User(
    val id: UserId,
    val nickname: String?,
)

@JvmInline
value class UserId(
    val value: Long,
)
```

and with nested generic types such as:

```kotlin
Map<UserId, List<Event?>>
```

Kotlin knows their nullability, generic arguments, constructor shape, value-class representation, and sealed hierarchy at compile time. Treating those types only as Java classes loses part of that information.

That can lead to extra `TypeInformation` boilerplate, runtime reflection, or generic serialization where a specialized serializer would be preferable.

Flinkt moves Kotlin-specific type analysis to compile time where practical and makes the resulting type information explicit at the Flink boundary.

Flinkt does not try to remove all reflection, only reflection on the per-record path:

```text
serialize
deserialize
copy
field access
comparison
```

Generated implementations should not require Kotlin reflection there.

## Design principles

### Keep Flink recognizable

Flinkt does not define a second streaming language.

If Flink calls an operation:

```text
map
filter
keyBy
process
connect
union
rebalance
```

Flinkt should use the same name unless Kotlin introduces a concrete ambiguity that cannot be resolved safely.

Existing Flink concepts such as operator UIDs, parallelism, state lifecycle, checkpoints, `ProcessFunction`, `TypeInformation`, and `TypeSerializer` remain visible.

### Make unsupported types explicit

A type that Flinkt cannot safely model should not silently become Kryo-serialized.

Generic serialization may be supported as an explicit opt-in, but it is not the default compatibility mechanism.

### Model Kotlin semantics

Shorter syntax is useful, but the main architectural reason for the library is the type layer. It is intended to understand Kotlin constructs such as:

- data classes;
- nullable types;
- generic types;
- value classes;
- enums;
- sealed hierarchies;
- Kotlin collections.

This list is the intended scope. What is actually supported will depend on the implementation and compatibility tests.

---

## DataStream façade

An extension function cannot replace Flink's existing `DataStream.map`, `keyBy`, and similar member functions. Kotlin gives callable members precedence over extensions.

Flinkt therefore uses thin Flink subtypes:

```text
DataStream<T>
     ▲
     │
FlinktDataStream<T>


SingleOutputStreamOperator<T>
     ▲
     │
FlinktSingleOutputStreamOperator<T>


KeyedStream<T, K>
     ▲
     │
FlinktKeyedStream<T, K>
```

These are still Flink streams.

A `FlinktDataStream<User>` is also a `DataStream<User>` and can be passed directly to existing Flink APIs.

The façade adds Kotlin overloads with the original operator names. Conceptually:

```kotlin
inline fun <reified R> map(
    transform: (T) -> R,
): FlinktSingleOutputStreamOperator<R>
```

can delegate to Flink with explicit output type information:

```text
Kotlin lambda
     │
     ├── MapFunction<T, R>
     │
     └── typeInfo<R>()
              │
              ▼
        Flink DataStream.map
```

Flink still owns the transformation and execution semantics.

### Enter once

Applications should enter the Kotlin façade at a boundary rather than opt in on every operation.

For example:

```kotlin
val env =
    StreamExecutionEnvironment
        .getExecutionEnvironment()
        .flinkt()
```

Streams created through that environment can remain Flinkt-aware:

```kotlin
val users =
    env.fromSource(
        source,
        watermarkStrategy,
        "users",
    )
        .map { normalize(it) }
        .filter { it.active }
        .keyBy { it.id }
```

An existing Flink stream can also be adapted explicitly:

```kotlin
val users = existingStream.flinkt()
```

Entering changes the Kotlin-facing API type and nothing Flink sees. It copies no records, adds no transformation, and keeps every check Flink would have applied to the original object.

For `env.flinkt()`, that means the result keeps working through the original environment. A `StreamExecutionEnvironment` holds the job's configuration and the list of transformations it will execute, so a second environment object would split the job in two.

### Adapting Flink stream objects

Sharing a stream's `Transformation` is not enough to make a new object behave like the original. Flink keeps some configuration on the stream object itself:

```text
SingleOutputStreamOperator   forceNonParallel() flag, side outputs already requested
KeyedStream                  key selector, key type, enableAsyncState() flag (2.x)
DataStreamSource             whether the source may run in parallel
```

Flink reads these fields when it validates configuration or builds later operators. After `op.forceNonParallel()`, `op.setParallelism(2)` fails. A second object built over `op`'s transformation starts with the flag cleared and would accept it. The public API exposes the key selector and key type but none of the other fields, and two objects can't share them. [State on stream objects](docs/flink-compatibility.md#state-on-stream-objects) records the fields for each target Flink line.

A keyed stream's partitioning can't be shared either. `KeyedStream`'s public constructors always add a new `PartitionTransformation`, and only a package-private `@Internal` constructor accepts an existing one. A rebuilt copy isn't always equivalent: `DataStreamUtils.reinterpretAsKeyedStream` keys a stream through a forward partitioner, and rebuilding that stream through the public constructor hash-partitions it again, adding the shuffle the user avoided on purpose.

Adaptation therefore follows the same rule as unsupported types. When the adapter can't reproduce a stream exactly, it fails with an explicit error instead of returning an approximation:

- **Non-keyed streams**, including an existing `SingleOutputStreamOperator`, adapt to a `FlinktDataStream<T>` over the same environment and transformation. Those are all that a downstream operator reads, so operators added after adaptation are the ones Flink would have added. The adapted value doesn't offer `name`, `uid`, `setParallelism`, or `getSideOutput`. The operator itself stays configured through the original reference, where Flink's checks live.
- **Keyed streams** are rejected, at compile time where the static type shows it. Streams keyed through the façade's own `keyBy` never need adapting. On a `KeyedStream` from elsewhere, Flink's own methods still work with an explicit `typeInfo<R>()`, and their results can enter the façade. Adapting the keyed stream itself would mean reaching Flink's package-private `@Internal` constructor, which the [dependency policy](docs/flink-compatibility.md#internal) discourages, and nothing needs that yet.
- **The runtime class decides**, not the static type. A `KeyedStream` passed around as `DataStream<T>` is still rejected. Keyed operators take their key from the `KeyedStream` object, so an unkeyed view would silently lose keyed state. A class the adapter doesn't recognize is rejected too, rather than treated as its nearest known superclass.

```kotlin
val users =
    parsed                // SingleOutputStreamOperator<Event>
        .uid("parse-v1")
        .setParallelism(4)
        .flinkt()         // FlinktDataStream<Event>
        .map { toUser(it) }
```

The façade's own operators wrap Flink objects too. The façade's `map` calls Flink's `map` and wraps the returned operator in a `FlinktSingleOutputStreamOperator`. Nothing else holds Flink's object, so no second reference can disagree, but the wrapper must start with the state Flink left on it. For example, Flink returns `windowAll` results already forced non-parallel. The façade's `keyBy` doesn't wrap anything. It builds its `FlinktKeyedStream` with the same public constructor that Flink's own `keyBy` uses, so it adds exactly one partitioning step.

### Preserve fluent chains

The façade only works if calls such as:

```kotlin
stream
    .map { normalize(it) }
    .name("normalize")
    .uid("normalize-v1")
    .setParallelism(8)
    .filter { it.valid }
```

do not fall back to a plain Java `SingleOutputStreamOperator` halfway through the chain.

Relevant fluent configuration methods therefore need covariant return types in the façade.

This creates maintenance work whenever Flink changes its fluent API. The cost is deliberate, and preferable to renamed operators throughout application code.

---

## Type information

The central low-level API is:

```kotlin
inline fun <reified T> typeInfo(): TypeInformation<T>
```

For example:

```kotlin
typeInfo<User>()

typeInfo<List<User>>()

typeInfo<Map<UserId, List<Event?>>>()

typeInfo<Envelope<User>>()
```

The resolution model is:

```text
Kotlin type
    │
    ▼
Flinkt type model
    │
    ├── Flink built-in type
    ├── collection type
    ├── generated Kotlin type
    └── explicit fallback
    │
    ▼
TypeInformation<T>
    │
    ▼
TypeSerializer<T>
```

`TypeInformation` and `TypeSerializer` remain separate layers.

Serializers are still created from Flink configuration. Flinkt should not replace that contract with globally constructed serializer singletons.

---

## Compile-time generated types

Types that need Kotlin-specific treatment can opt into generation:

```kotlin
@FlinkType
data class User(
    val id: UserId,
    val name: String,
    val email: String?,
)
```

KSP has enough information to generate direct field access:

```kotlin
value.id
value.name
value.email
```

and direct construction:

```kotlin
User(
    id = idSerializer.deserialize(input),
    name = nameSerializer.deserialize(input),
    email = emailSerializer.deserialize(input),
)
```

The generated hot path should not need operations such as:

```text
KProperty lookup
memberProperties
primaryConstructor.call(...)
componentN reflection lookup
java.lang.reflect.Field.get(...)
```

Reflection during type discovery or application startup is acceptable.

KSP is build-time tooling and should not become a runtime dependency.

---

## Generated serializers

The serializer is one of the places where per-type code generation is justified.

A generic serializer that repeatedly moves data through `Array<Any?>` may remove reflection while still creating avoidable allocation and boxing.

For:

```kotlin
@FlinkType
data class User(
    val id: Long,
    val name: String,
)
```

a generated serializer should be able to write fields directly:

```kotlin
override fun serialize(
    value: User,
    out: DataOutputView,
) {
    idSerializer.serialize(value.id, out)
    nameSerializer.serialize(value.name, out)
}
```

and construct the result directly during deserialization.

Shared Flink protocol behavior should remain centralized where possible. Generating an entirely independent `TypeInformation`, comparator, and snapshot implementation for every Kotlin class would create more persisted and version-sensitive surface area than is necessary to remove reflection.

---

## Nullability

Kotlin nullability is part of a type's schema.

These are not equivalent:

```kotlin
String
String?
```

For example:

```kotlin
@FlinkType
data class User(
    val id: Long,
    val nickname: String?,
    val age: Int?,
)
```

gives the processor enough information to generate a serializer without discovering nullability at runtime.

A compact null bitmap is one possible binary representation, but the exact representation is a state-compatibility decision rather than an API detail.

Changing `String` to `String?` must not be declared compatible merely because both types use the same JVM class.

---

## Value classes

Keeping a domain type as a value class should not necessarily add serialization overhead.

For example:

```kotlin
@JvmInline
value class UserId(
    val value: Long,
)
```

may be represented by its underlying `Long` serializer when that representation is safe.

This allows:

```kotlin
UserId
OrderId
PaymentId
```

to remain distinct application types without forcing generic serialization for each wrapper.

Whether a representation change is compatible with previously persisted state remains a serializer-snapshot concern.

---

## Generic types

Generic Kotlin types must retain their concrete type arguments.

Given:

```kotlin
@FlinkType
data class Envelope<T>(
    val timestamp: Long,
    val payload: T,
)
```

this:

```kotlin
typeInfo<Envelope<User>>()
```

must model:

```text
Envelope
└── T = User
```

rather than treating `payload` as `Any`.

The same applies to collections:

```kotlin
List<User>
List<User?>
Map<UserId, User>
Map<UserId, List<Event?>>
```

Resolving only `T::class.java` is insufficient for these cases because JVM classes alone do not preserve the complete Kotlin type.

---

## Sealed types and enums

Sealed hierarchies can be represented as tagged unions:

```kotlin
@FlinkType
sealed interface Event

@FlinkType
data class UserCreated(
    val userId: UserId,
) : Event

@FlinkType
data class UserDeleted(
    val userId: UserId,
) : Event

@FlinkType
data object Shutdown : Event
```

Subtype identity must be stable across builds. Source declaration order is not a sufficient persistent identity.

Enums have the same problem: serializing an enum only by ordinal makes insertion and reordering hazardous for persisted state.

Flinkt's schema model should therefore distinguish logical identity from source position. The concrete stable-ID policy remains part of the serializer compatibility design and should be reviewed before it becomes a persisted format.

---

## State

State helpers use the same type-resolution path.

Instead of reducing a generic Kotlin type to its raw JVM class:

```kotlin
ValueStateDescriptor(
    "users",
    List::class.java,
)
```

Flinkt can preserve the complete type:

```kotlin
runtimeContext.valueState<List<User>>("users")
```

Likewise:

```kotlin
runtimeContext.listState<Event>("events")

runtimeContext.mapState<UserId, User>("users")
```

The resulting descriptors remain normal Flink state descriptors.

State lifecycle is intentionally not hidden. Initialization still happens where Flink makes runtime state available:

```kotlin
class UserFunction :
    RichMapFunction<Event, User>() {

    private lateinit var users:
        MapState<UserId, User>

    override fun open(
        openContext: OpenContext,
    ) {
        users =
            runtimeContext.mapState("users")
    }

    override fun map(
        value: Event,
    ): User {
        // ...
    }
}
```

A property-delegate DSL could make this shorter, but it would also make the runtime-context binding point less obvious. That trade-off is not part of the initial design.

---

## Generated type registry

Generated types need to work across module boundaries without scanning the entire classpath.

A module can expose its generated types through a Flinkt-owned SPI:

```kotlin
interface GeneratedTypeModule {

    fun register(
        registry: MutableTypeRegistry,
    )
}
```

Per-type generated code remains associated with its source declaration, while a module-level registry aggregates the types produced by that compilation.

A consuming module should reuse generated metadata from a dependency rather than regenerate serializers for classes it does not own.

The registry belongs to Flinkt. It should not depend on undocumented Flink discovery behavior.

---

## Schema and state compatibility

Compile-time schema comparison and Flink state compatibility solve different problems.

A schema tool can detect changes such as:

```text
field added
field removed
field renamed
field reordered
type changed
nullability changed
subtype changed
```

but detection alone does not prove that the new serializer can read the previous binary representation.

Flink checkpoint and savepoint compatibility is governed by `TypeSerializer` and `TypeSerializerSnapshot`.

For that reason, the conservative starting point is strict compatibility: structural changes are incompatible until the serializer has a defined and tested migration path.

Serializer-format decisions become difficult to reverse after released applications have persisted state.

The preferred long-term trade-off is:

```text
compact per-record representation
              +
schema-rich serializer snapshot
              +
migration work during restore when necessary
```

rather than writing field names or IDs into every record merely to make future migration easier.

The exact evolution matrix is still open. It should be defined alongside the implemented serializer format and verified against state written by previous versions.

---

## Table API

Table types carry information that is not identical to DataStream `TypeInformation`.

For example:

```kotlin
data class User(
    val id: Long,
    val name: String?,
)
```

contains enough Kotlin information to model:

```text
id    BIGINT NOT NULL
name  STRING
```

A Table integration can therefore consume the same Kotlin schema model while mapping it independently to Flink `DataType` and `Schema`.

Potential low-level APIs are:

```kotlin
inline fun <reified T> dataType(): DataType

inline fun <reified T> tableSchema(): Schema
```

The Table layer should not treat `DataType` as another spelling of `TypeInformation`.

---

## Project boundaries

The architecture separates compile-time analysis, runtime type support, and user-facing DataStream integration.

Conceptually:

```text
@FlinkType source
       │
       ▼
      KSP
       │
       ├── schema metadata
       ├── generated adapter
       ├── generated serializer
       └── module registry
       │
       ▼
Flinkt type runtime
       │
       ├── typeInfo<T>()
       ├── TypeInformation
       ├── serializer snapshots
       └── generated registry
       │
       ▼
DataStream façade / state / Table integration
       │
       ▼
Apache Flink
```

The layers are separate because KSP compatibility, public Kotlin APIs, and persisted Flink serializer formats evolve on different timescales.

A change to generated source code can be cheap to replace. A change to a serializer format after users have produced checkpoints may not be.

---

## Why no compiler plugin?

A Kotlin compiler plugin could make a raw Flink `DataStream<T>` transparently receive explicit Kotlin-aware type information:

```kotlin
stream.map {
    User(...)
}
```

without a façade.

That provides an attractive source experience, but it also couples the project to Kotlin compiler internals and compiler-version compatibility.

The subtype façade reaches most of the same user-facing syntax using ordinary Kotlin/JVM mechanisms.

A compiler plugin may become an optional layer later. It is not a foundation of the initial architecture.

---

## Initial implementation boundary

The first useful end-to-end case is intentionally narrow:

```kotlin
@FlinkType
data class User(
    val id: Long,
    val name: String,
)
```

and:

```kotlin
val users =
    events
        .map {
            User(
                id = it.id,
                name = it.name,
            )
        }
        .name("users")
        .keyBy { it.id }
```

with state:

```kotlin
runtimeContext.valueState<User>("user")
```

That vertical slice should establish the contracts that later features depend on:

- Kotlin operator overloads provide explicit Flink type information;
- fluent Flink calls preserve the façade;
- entering the façade, and wrapping the streams Flink returns, leave the stream graph unchanged and drop none of the state Flink keeps on stream objects;
- generated record serialization does not use Kotlin reflection;
- generated types do not silently become generic/Kryo types;
- normal Flink APIs can consume Flinkt stream subtypes;
- serializer snapshots can restore state correctly.

Nullable fields, collections, value classes, generic classes, sealed hierarchies, and schema migration should build on those contracts rather than bypass them.

---

## Review focus

Review should focus on the boundaries where Kotlin convenience can accidentally change Flink behavior.

**API resolution:** Does `stream.map { ... }` select the intended Kotlin overload? Can the underlying Flink overload still be used deliberately?

**Façade preservation:** Do `name`, `uid`, `setParallelism`, and related calls keep the stream in the Flinkt type hierarchy?

**Adaptation:** Does entering the façade, or wrapping a stream Flink returned, keep the same transformation and all the state Flink holds on the original object? Where it can't, does it fail explicitly?

**Type propagation:** Whenever an operator introduces a new generic type, does Flink receive the complete `TypeInformation`, including nested generic arguments and nullability where relevant?

**Hot path:** Can serialization, deserialization, copying, field access, or comparison reach Kotlin reflection indirectly?

**Interoperability:** Can Flinkt streams be used directly wherever ordinary Flink `DataStream` and `KeyedStream` values are expected?

**State compatibility:** Does every compatibility result correspond to bytes that the new serializer can actually consume? This deserves more scrutiny than ordinary API code because persisted state outlives a process and often outlives a library release.

---

## Non-goals

Flinkt is not intended to replace Apache Flink's runtime or programming model.

It does not aim to hide state lifecycle, redefine every Flink operator, silently serialize unsupported objects with Kryo, or promise compatibility across arbitrary Flink versions without a tested compatibility matrix.

The user-facing test for the design is whether a Flink developer still recognizes the code as Flink.