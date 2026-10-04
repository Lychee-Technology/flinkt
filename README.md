# Flinkt

**Kotlin-first APIs and compile-time type support for Apache Flink.**

Flinkt is a Kotlin-focused integration layer for Apache Flink. It keeps Flink's programming model and operator vocabulary, while addressing the places where Java-oriented type extraction and APIs do not map cleanly to Kotlin.

Application code should look like ordinary Flink code:

```kotlin
val sessions =
    env.fromSource(
        source,
        watermarkStrategy,
        "events",
    )
        .flinkt()
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

sessions.sinkTo(sink)
```

`.flinkt()` gives Kotlin a view of the Flink stream. From there on, every operator that produces a new element type passes Flink the complete Kotlin type. `.asFlink()` hands back the Flink object when a Flink API needs it.

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
union
rebalance
```

Flinkt uses the same name unless Kotlin introduces a concrete ambiguity that cannot be resolved safely.

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
- Kotlin collections: `List` and `Map` in the first release.

This list is the intended scope. What is actually supported will depend on the implementation and compatibility tests.

---

## Flinkt streams

### A view of one Flink object

`.flinkt()` gives a Kotlin view of a Flink stream. Kotlin chooses the view from the static type:

| Flink object | View |
|---|---|
| `DataStream<T>` | `FlinktDataStream<T>` |
| `SingleOutputStreamOperator<T>` | `FlinktSingleOutputStreamOperator<T>` |
| `KeyedStream<T, K>` | `FlinktKeyedStream<T, K>` |

Any object of these classes can enter: a source, a side output, the result of `union`, a keyed stream built anywhere, or a subclass. The view holds that object and nothing else, and `asFlink()` returns the same instance with its Flink type:

```kotlin
val parsed: SingleOutputStreamOperator<Event> = ...

val events = parsed.flinkt()        // FlinktSingleOutputStreamOperator<Event>
check(events.asFlink() === parsed)
```

Entering creates no Flink object, adds no transformation, and reads nothing from the stream. Everything Flink knows about the stream stays on Flink's object: its partitioning, a `forceNonParallel()` flag, the side outputs already requested. A view is not a `DataStream`. To pass one to an API that expects a `DataStream`, call `asFlink()`.

An earlier version of this design made each Flinkt stream a subclass of the Flink class. A subclass has to be a second Flink object, and Flink keeps state on stream objects that a second object doesn't have. A subclass also inherits every Flink method under the same name, so users couldn't tell which calls get Kotlin types. [Architecture](docs/architecture.md#decision-streams-are-views-not-flink-subtypes) records the evidence and the alternatives.

### Flink's names, Flink's behavior

Each view method makes one call to the Flink method of the same name on the object it holds, and wraps the object Flink returns. Flink's own checks apply. After `op.forceNonParallel()`, `op.flinkt().setParallelism(2)` fails exactly as `op.setParallelism(2)` does.

Fluent configuration returns the view, so a chain stays in Flinkt. The same holds for the other Flink methods that keep the element type, such as `filter`, `union` and repartitioning, and for terminal calls such as `sinkTo`, `print` and `executeAndCollect`:

```kotlin
stream
    .map { normalize(it) }
    .name("normalize")
    .uid("normalize-v1")
    .setParallelism(8)
    .filter { it.valid }
```

These forwarding methods are generated from Flink's own classes when Flinkt is built for each Flink line. They keep Flink's overloads and parameter names. A method Flink deprecates stays deprecated, and a method Flink marks `@Experimental` needs `@OptIn(ExperimentalFlinkApi::class)`. The generator refuses any method that would break the rules on this page ([Generated forwarders](docs/architecture.md#generated-forwarders)).

### Where result types come from

Each operator that introduces an element type has one overload, and it takes Flink's own function type. A Kotlin lambda converts to that type, and a Flink function object is passed as it is. Either way, the compiler knows the result type at the call site, and Flinkt passes `typeInfo<R>()` for it to Flink:

```kotlin
events.map { User(it.id, it.name) }     // R = User, from the lambda
events.map(ToUser())                    // R = User, from ToUser : MapFunction<Event, User>
users.keyBy { it.id }                   // K = Long
keyed.process(SessionFunction())        // R = Session, from SessionFunction : KeyedProcessFunction<Long, User, Session>
```

When the type isn't known at the call site, for example inside generic code, the call doesn't compile. The overload with an explicit `TypeInformation` covers that case:

```kotlin
fun <R> enrich(
    events: FlinktDataStream<Event>,
    fn: MapFunction<Event, R>,
    type: TypeInformation<R>,
) = events.map(fn, type)
```

Nullability comes through the same way. A lambda that returns `User?` produces a stream of `User?`, which Flinkt models differently from `User`. A function written in Java has no nullability information, so Flinkt treats its result as non-null. When a Java function can return null, state the type: `map<User?>(javaFunction)`.

### Leaving the view

Only the methods a view offers are Flinkt's. For anything else, such as `connect`, windows, joins, broadcast state, side outputs, or a library that takes a `DataStream`, call `asFlink()`. From that point on, Flink's rules apply, including its own type inference. Pass `typeInfo<R>()` wherever Flink accepts a `TypeInformation`, and re-enter with `.flinkt()`:

```kotlin
val counts =
    users                               // FlinktKeyedStream<User, Long>
        .asFlink()
        .window(windowAssigner)
        .aggregate(CountVisits(), typeInfo<VisitCount>(), typeInfo<UserVisits>())
        .flinkt()
        .name("visits")
```

A method a view doesn't offer is a compile error, never a silent switch to Flink's method. Views cover more of Flink's API over time, and each one is reachable through `asFlink()` until then.

Code outside the view can still produce a Kryo type. Setting Flink's `pipeline.generic-types` to `false` makes Flink reject generic types when it builds the job. Flinkt recommends that setting but doesn't set it itself, because using Flinkt changes nothing Flink sees.

### When something isn't supported

Flinkt fails at the earliest point that can see the problem:

| Situation | Fails | Result |
|---|---|---|
| A `@FlinkType` declaration uses a type Flinkt can't model | at compile time (KSP) | an error naming the field and the type |
| An operator's result type isn't modeled | when the job graph is built (`typeInfo<R>()`) | an error naming the full Kotlin type and the two fixes: annotate it with `@FlinkType`, or pass a `TypeInformation` explicitly |
| An operator's result type isn't known at the call site | at compile time | use the overload that takes a `TypeInformation` |
| The view doesn't offer a Flink method | at compile time | call it on `asFlink()` |
| The Flinkt adapter doesn't match the Flink runtime | on first use of the adapter | a message naming the adapter for the running Flink line |

Flinkt vouches for the types it produces. It doesn't inspect a stream's type on entry, because reading the type makes Flink refuse a later `returns()` on the original stream.

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

`typeInfo<T>()` reads the Kotlin type with `typeOf<T>()`, which carries generic arguments and nullability and doesn't need `kotlin-reflect`.

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

Shared Flink protocol behavior stays centralized. Generating an entirely independent `TypeInformation`, comparator, and snapshot implementation for every Kotlin class would create more persisted and version-sensitive surface area than is necessary to remove reflection. [Generated code and the adapter](docs/architecture.md#generated-code-and-the-adapter) describes the split.

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

The planned representation is one null bitmap per record for its nullable fields, which a record without nullable fields doesn't carry, and a marker in front of a top-level nullable value. Its exact bytes are a state-compatibility decision ([#5](https://github.com/Lychee-Technology/flinkt/issues/5)), not an API detail.

Changing `String` to `String?` must not be declared compatible merely because both types use the same JVM class. It's a type change, and incompatible with persisted state.

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

may be written with exactly the bytes of its underlying `Long` when that representation is safe.

This allows:

```kotlin
UserId
OrderId
PaymentId
```

to remain distinct application types without forcing generic serialization for each wrapper.

The bytes carry no wrapper, but the serializer snapshot records it, so `Long`, `UserId` and `OrderId` stay different schemas. Changing a value class's underlying type is incompatible with previously persisted state. So is switching a field between `UserId` and `Long`, even though the bytes match ([Schema and state compatibility](#schema-and-state-compatibility)). A value class over an eligible type, such as `UserId`, can be a key ([Keys](#keys)). Which value classes use this representation is still being decided ([#18](https://github.com/Lychee-Technology/flinkt/issues/18)).

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

The first release models `List` and `Map`, as record fields and as top-level types, with one encoding for both. `Set`, `Collection`, the mutable interfaces such as `MutableList`, and arrays fail explicitly. A decoded `List` is an `ArrayList`, and a decoded `Map` is a `LinkedHashMap` that keeps the order its entries were written in. Two equal maps built in different orders can serialize to different bytes, so a `Map` is never a key ([Collections](docs/architecture.md#collections)).

---

## Sealed types and enums

Sealed hierarchies are written as tagged unions, and enums are written as tags. Every subtype and every enum constant declares its tag with `@FlinkId`:

```kotlin
@FlinkType
sealed interface Event

@FlinkType
@FlinkId(1)
data class UserCreated(
    val userId: UserId,
) : Event

@FlinkType
@FlinkId(2)
data class UserDeleted(
    val userId: UserId,
) : Event

@FlinkType
@FlinkId(3)
data object Shutdown : Event

@FlinkType
enum class Priority {
    @FlinkId(1) LOW,
    @FlinkId(2) HIGH,
}
```

Declaration order and ordinals aren't stable. Inserting or reordering a subtype or constant would make old state decode as a different value, with no error. A tag derived from the name would change silently on a rename. An explicit ID survives both, so subtypes and constants can be renamed, reordered or moved between files without changing their bytes. An ID is permanent: don't give a retired ID to a different constant or subtype, because Flinkt can't tell that from a rename.

A missing or duplicate ID is a compile error. Reading an ID the current code doesn't declare fails, and any change to the set of IDs is incompatible. A change to a subtype's own fields follows the rules for records ([Schema and state compatibility](#schema-and-state-compatibility)). Enum constants with bodies, `object` and `data object` subtypes, and nested sealed hierarchies are supported, and each nested level numbers its own subtypes. [Enum and sealed identity](docs/architecture.md#enum-and-sealed-identity) gives the encoding.

An enum constant, an `object` and a `data object` are written as their ID alone, and reading one returns the constant or singleton of the JVM that reads it. None of its properties are written. So an `object` or `data object` subtype is a stateless marker: a stored property on it is a compile error, and a subtype that carries data is a data class. An enum can declare `val`s, as in `Priority(val weight: Int)`, because every JVM builds them from the declaration. A stored `var` on an enum is a compile error.

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

Flinkt preserves the complete type. There are two helper families, named after the Flink type each one returns.

Descriptor helpers build an ordinary Flink descriptor, for example to configure TTL or to pass to a Flink API that takes one:

```kotlin
valueStateDescriptor<List<User>>("users")      // ValueStateDescriptor<List<User>>
listStateDescriptor<Event>("events")           // ListStateDescriptor<Event>
mapStateDescriptor<UserId, User>("users")      // MapStateDescriptor<UserId, User>
```

Binding helpers call Flink's `getState`, `getListState`, or `getMapState` with such a descriptor, and return Flink's state handle:

```kotlin
runtimeContext.valueState<User>("user")        // ValueState<User>
runtimeContext.listState<Event>("events")      // ListState<Event>
runtimeContext.mapState<UserId, User>("users") // MapState<UserId, User>
```

State lifecycle is intentionally not hidden. Binding happens where Flink makes runtime state available, and the type arguments come from the property:

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

## Keys

A type Flinkt can serialize isn't automatically a safe key. Flink picks a partition key's key group from its `hashCode()` and finds RocksDB state by its bytes, and checkpoints persist both. A key whose hash code differs in the JVM that restores the job loses its state, and nothing reports an error. Flinkt accepts a modeled type as a key only where it can check mechanically that this can't happen:

| Key type | `keyBy` | `mapState<K, V>()` user key | broadcast-state key |
|---|---|---|---|
| `Int`, `Long`, `String` and the other non-null primitives | yes | yes | yes |
| value class over an eligible type, such as `UserId(val value: Long)` | yes | yes | yes |
| `@FlinkType` data class whose properties are all eligible, such as `UserKey(val tenant: Long, val id: Long)` | yes | yes | yes |
| enum | no | yes\* | yes\* |
| `object`, `data object`, sealed type, `List`, `Map`, nullable `K?` | no | no | no |

\* Once a restore test in a second JVM confirms it ([Eligibility](docs/architecture.md#eligibility)).

The rule is recursive. A data class or value class is a key only if every property is, so one enum property makes a data class usable as a `MapState` user key but not in `keyBy`. A data class that overrides `equals()` or `hashCode()` isn't a key anywhere. Flinkt can't tell whether a hand-written hash code is the same in every JVM, or will stay the same in the next release. An enum's `@FlinkId` makes its bytes stable, but `Enum.hashCode()` is an identity hash code, so an enum isn't a partition key.

Changing the `equals()` or `hashCode()` of a type that already keys persisted state is a breaking change, even when its schema stays the same, and no serializer snapshot can see it. Through the view, adding such an override makes the type ineligible, so the upgraded job fails when its graph is built.

A key's schema doesn't evolve. A change that would migrate a `ValueState<User>`, such as removing a field, fails the restore when `User` is a `keyBy` key, a `MapState` user key or a broadcast-state key. `mapStateDescriptor` and `runtimeContext.mapState` record in the serializer snapshot that the map's key is a key. A `MapStateDescriptor` built by hand from `typeInfo<K>()` doesn't, and Flink's heap backend and broadcast state would then accept a value migration of that key ([Schema evolution](docs/schema-evolution.md#keys)).

A rejected key fails when the job graph is built for `keyBy`, or in `open()` for `mapState`. The message names the type, the context and the reason. Two exits skip Flinkt's key checks on purpose: `keyBy` with a `TypeInformation` that Flinkt didn't build, and anything called through `asFlink()`. Flink's own validation then applies, and so do Flink's guarantees rather than Flinkt's. `mapStateDescriptor<K, V>()` checks no key rule, because its descriptor may become broadcast state. Nothing checks broadcast-state keys yet, because broadcast is reached through `asFlink()`. [Keys](docs/architecture.md#keys) gives the reasons.

---

## Generated type registry

Generated types need to work across module boundaries without scanning the entire classpath.

A module exposes its generated types through a Flinkt-owned SPI:

```kotlin
interface GeneratedTypeModule {

    fun register(
        registry: MutableTypeRegistry,
    )
}
```

KSP generates one implementation per compilation and registers it in `META-INF/services`, where `java.util.ServiceLoader` finds it. Per-type generated code remains associated with its source declaration, while the module-level registry aggregates the types produced by that compilation.

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

For ordinary persisted values, such as a `ValueState` value, a `ListState` element or a `MapState` value, Flinkt's target is the schema evolution Flink offers for POJOs:

| Change | Result |
|---|---|
| a field removed | migrates, and the old value is dropped |
| a field added, with a defined migration default | migrates |
| a field added without one, such as a non-null `String` | incompatible |
| a field renamed | a removal plus an addition |
| constructor parameters reordered | compatible as-is, because fields are persisted in an order derived from their names ([#5](https://github.com/Lychee-Technology/flinkt/issues/5)) |
| a field's type changed, including its nullability | incompatible |
| the class renamed or moved to another package | incompatible |
| a nested record, `List` element or `Map` value changes | the same rules, recursively |

An added nullable field starts as `null`. Any other added field needs a migration default that its declaration states, because Kotlin has nothing like Java's default for a non-null `String`, an enum or a validated value class. A default argument in the constructor isn't used, because it can compute a different value on each restore ([#37](https://github.com/Lychee-Technology/flinkt/issues/37)).

Migration is built in stages. A change stays incompatible until the stage that supports it lands with its evidence, and restore then stops with an error instead of reading state it might misinterpret. [Stages](docs/schema-evolution.md#stages) says which changes each stage enables. Renaming or reordering enum constants and sealed subtypes that keep their IDs isn't a change. Adding or removing one is incompatible.

Keys don't evolve. A structural change to a `keyBy` key, a `MapState` user key, a broadcast-state key, or the key type of a `Map`, fails the restore on every backend ([Keys](#keys)). An unchanged schema is necessary for a key but not sufficient, because a key's identity also includes its `equals()` and `hashCode()`.

Serializer-format decisions become difficult to reverse after released applications have persisted state. The trade-off is:

```text
compact per-record representation
              +
schema-rich serializer snapshot
              +
migration work during restore when necessary
```

rather than writing field names or IDs into every record merely to make future migration easier. From the first release on, the snapshot records the full schema, so a migration reads old bytes without the classes that wrote them, and the class of a removed field can leave the job's jar. [Schema evolution](docs/schema-evolution.md) gives the reasons, and [testing.md](docs/testing.md#serializer-snapshot-compatibility) the evidence each result needs.

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

The architecture separates compile-time analysis, runtime type support, and user-facing Flink integration:

```text
@FlinkType source
       │
       ▼
      KSP                          build time only
       │
       ├── schema metadata
       ├── generated codec         no Flink types
       └── module registry
       │
       ▼
flinkt-core                        type model, codec SPI; no Flink dependency
       │
       ▼
Flink adapter (one per line)       views, typeInfo<T>(), TypeInformation,
       │                           serializer snapshots, state helpers
       ▼
Apache Flink
```

The layers are separate because KSP compatibility, public Kotlin APIs, and persisted Flink serializer formats evolve on different timescales. A change to generated source code can be cheap to replace. A change to a serializer format after users have produced checkpoints may not be. [Architecture](docs/architecture.md#layers-and-modules) lists the modules, and [Flink version compatibility](docs/flink-compatibility.md) covers the adapters.

---

## Why no compiler plugin?

A Kotlin compiler plugin could give a raw Flink `DataStream<T>` Kotlin-aware type information:

```kotlin
stream.map {
    User(...)
}
```

without a view.

That provides an attractive source experience, but it also couples the project to Kotlin compiler internals and compiler-version compatibility.

The view reaches the same source with ordinary Kotlin: `.flinkt()` where a stream enters, and `asFlink()` where it leaves.

A compiler plugin may become an optional layer later, for example to flag raw Flink calls that produce types Flinkt models. It is not a foundation of the initial architecture.

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
        .flinkt()
        .map {
            User(
                id = it.id,
                name = it.name,
            )
        }
        .name("users")
        .keyBy { it.id }
        .process(UserFunction())
```

with state bound in `UserFunction.open()`:

```kotlin
user = runtimeContext.valueState<User>("user")
```

The slice is built against Flink 2.3 first. From the first slice on, its adapter source also compiles against Flink 1.20, where the Flink type protocol differs the most ([CI tiers](docs/testing.md#pull-requests)).

That vertical slice should establish the contracts that later features depend on:

- view operators give Flink explicit type information derived from the call site;
- fluent Flink calls return the view, and every forwarding method is generated from the adapter line's Flink classes;
- entering a view adds nothing to the job, and the view makes the same Flink calls as direct Flink code;
- `asFlink()` returns the original Flink object;
- generated record serialization does not use Kotlin reflection;
- generated types do not silently become generic/Kryo types;
- serializer snapshots can restore state correctly.

Nullable fields, collections, value classes, generic classes, sealed hierarchies, schema migration, and views over more of Flink's API should build on those contracts rather than bypass them.

---

## Review focus

Review should focus on the boundaries where Kotlin convenience can accidentally change Flink behavior.

**API resolution:** Does `stream.map { ... }`, or `stream.map(fn)`, select the view's overload, with the result type taken from the call site?

**View discipline:** Does each view method make exactly one call on the Flink object the view holds, and wrap what Flink returned? Does any Flinkt code construct a Flink stream object, or read the type of a stream a view wraps?

**Type propagation:** Whenever an operator introduces a new generic type, does Flink receive the complete `TypeInformation`, including nested generic arguments and nullability where relevant? Does the adapter reach it through a public Flink method?

**Hot path:** Can serialization, deserialization, copying, field access, or comparison reach Kotlin reflection indirectly?

**Interoperability:** Does `asFlink()` return the original object, so every Flink API still accepts it?

**State compatibility:** Does every compatibility result correspond to bytes that the new serializer can actually consume? Is every migration the snapshot promises one the old-layout reader performs, shown on both state backends? Can a value migration reach a key? This deserves more scrutiny than ordinary API code because persisted state outlives a process and often outlives a library release.

**Keys:** Does every key rule rest on something Flinkt can check mechanically, rather than on the user's `hashCode()`? Would a key whose hash code or bytes differ in the restoring JVM be rejected before it's used?

---

## Non-goals

Flinkt is not intended to replace Apache Flink's runtime or programming model.

It does not aim to hide state lifecycle, redefine every Flink operator, silently serialize unsupported objects with Kryo, or promise compatibility across arbitrary Flink versions without a tested compatibility matrix.

The user-facing test for the design is whether a Flink developer still recognizes the code as Flink.
