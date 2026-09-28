# AGENTS.md

## Project state

**Flinkt** is in the design stage. The repository has the design (`README.md`), the Flink version policy (`docs/flink-compatibility.md`), the test strategy (`docs/testing.md`), and contributor rules (`docs/non-code-rules.md`), but no source code, build files, or tests yet, so there are no build, lint, or test commands. Don't make them up. When a build is added (`.gitignore` expects Kotlin/Gradle), record the commands here, including how to run a single test.

`README.md` is the design document. Read it before you propose or write code. It says which decisions are settled and which are deliberately left open.

Read `docs/flink-compatibility.md` before you change Flink-facing code or the support matrix. Read `docs/testing.md` before you add tests or CI. It defines the evidence that each compatibility claim needs.

## Architecture (planned)

Flinkt is a Kotlin-first layer over Apache Flink APIs. It has three layers, and each one changes on its own timescale. Generated source is cheap to replace. A serializer format that users have already written into checkpoints is not.

1. **KSP processor (build time only).** Reads `@FlinkType` declarations and generates schema metadata, per-type adapters and serializers, and a module-level registry that implements the Flinkt-owned `GeneratedTypeModule` SPI. The registry means Flinkt never scans the classpath or depends on undocumented Flink discovery. A consuming module reuses the generated metadata from a dependency; it doesn't regenerate code for classes it doesn't own. KSP must never become a runtime dependency.
2. **Type runtime.** `inline fun <reified T> typeInfo(): TypeInformation<T>` passes a Kotlin type through the Flinkt type model and resolves it to a Flink built-in type, a collection type, a generated type, or an explicit fallback. `TypeInformation` and `TypeSerializer` stay separate. Serializers are created through Flink configuration, not as global singletons. Only the per-record serializer is generated per type. Shared protocol code (`TypeInformation`, comparators, snapshots) stays in one central place.
3. **Integration.**
   - DataStream façade (below).
   - State helpers: `runtimeContext.valueState<T>()`, `listState`, and `mapState` return ordinary Flink descriptors.
   - Table integration: `dataType<T>()` and `tableSchema<T>()` map the same schema model to `DataType`/`Schema` on their own. `DataType` is not another spelling of `TypeInformation`.

**DataStream façade.** The façade uses thin subtypes: `FlinktDataStream<T> : DataStream<T>`, `FlinktSingleOutputStreamOperator<T> : SingleOutputStreamOperator<T>`, and `FlinktKeyedStream<T, K> : KeyedStream<T, K>`. These add reified overloads under Flink's own operator names, and the overloads pass `typeInfo<R>()` to Flink. Applications enter the façade once, through `env.flinkt()` or `stream.flinkt()`. An existing `SingleOutputStreamOperator` enters as a `FlinktDataStream`, so the operator's own configuration stays on the original reference. An existing `KeyedStream` can't enter at all (README, "Adapting Flink stream objects"). Fluent configuration methods (`name`, `uid`, `setParallelism`, …) need covariant overrides so a chain never drops back to a plain Flink type partway through. Those overrides need upkeep whenever Flink's fluent API changes, and the design accepts that cost rather than renaming operators. There is no Kotlin compiler plugin. One may be added later as an optional layer.

## Invariants

Code and reviews are held to these rules (see "Review focus" in the README):

- **Flink stays recognizable.** Use Flink's operator names. Rename one only when Kotlin creates a concrete ambiguity that can't be resolved safely. Keep UIDs, parallelism, `ProcessFunction`, `TypeInformation`, and `TypeSerializer` visible. State is bound in `open()`, not hidden behind property delegates.
- **No silent Kryo.** A type Flinkt can't model fails explicitly. Generic serialization is opt-in only.
- **No reflection on the per-record path**: serialize, deserialize, copy, field access, or comparison. Reflection during type discovery or startup is fine. Also avoid generic `Array<Any?>`-style serializers, because they box and allocate.
- **Keep the full Kotlin type.** That includes nested generic arguments and nullability. `T::class.java` is not enough. Changing `String` to `String?` changes the schema. It is not compatible just because the JVM class is the same.
- **Stable identity.** Sealed subtypes and enums need logical IDs. Declaration order and enum ordinals are not enough.
- **Strict state compatibility.** Treat every structural change as incompatible until its migration path is defined and tested against state written by earlier versions. A "compatible" result must mean the new serializer can actually read the old bytes. Prefer compact records, a schema-rich `TypeSerializerSnapshot`, and migration on restore. Don't write field names or IDs into every record. Serializer-format changes need more scrutiny than API changes.
- **Interoperability.** A Flinkt stream must work anywhere Flink expects a `DataStream` or `KeyedStream`.
- **Entering the façade changes nothing Flink sees.** Entering shares the original's transformation, copies no records, and neither drops nor splits the state Flink holds on the stream object, such as the `forceNonParallel()` flag or a keyed stream's partitioning. The same goes for the Flink objects that the façade's own operators wrap. Where Flink's public API can't reproduce a stream exactly, fail explicitly rather than approximate.

Still undecided, so don't present these as settled: the stable-ID policy, the null-bitmap representation, when a value class can use its underlying type's serializer, and the schema-evolution matrix.

**First milestone.** It is a deliberately narrow vertical slice: `@FlinkType data class User(val id: Long, val name: String)`, then `map { User(...) }.name(...).keyBy { it.id }` through the façade, then `runtimeContext.valueState<User>("user")`, with serializer snapshots restoring state correctly. The milestone doesn't name a Flink line yet. Which adapter is built first, and which one PR CI treats as primary, is tracked in [#2](https://github.com/Lychee-Technology/flinkt/issues/2). Nullable fields, collections, value classes, generics, sealed hierarchies, and migration come later and build on the contracts this slice sets.

## Non-code artifacts

Issues, PR descriptions, design docs, plans, and reviews follow `docs/non-code-rules.md`. The main points:

- Write for a reader who wasn't in the session. Cover the why, the outcome, the judgment calls, the risks, and what reviewers should focus on.
- Record reasoning, not a narrated diff. Never invent measurements or decisions.
- Every artifact must end up on GitHub, in English. Issue and PR descriptions are the issue or PR body. Post any other artifact in full as a comment on the relevant PR or issue, and say what kind of artifact it is. A file path or summary doesn't count.
- Durable decisions such as ADRs and runbooks go in `docs/`.

Blog posts in `docs/blog/` also follow `docs/blog/AGENTS.md`, which doesn't exist yet.
