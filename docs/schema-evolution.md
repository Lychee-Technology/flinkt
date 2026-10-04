# Schema evolution

This document records how Flinkt decides whether persisted state can be restored after a schema change, and how it migrates the state that can be. It is the decision record for [#25](https://github.com/Lychee-Technology/flinkt/issues/25). [architecture.md](architecture.md#schema-evolution) gives the overview, and [testing.md](testing.md#serializer-snapshot-compatibility) defines the evidence each result needs.

## Decision history

**First decision: strict** ([#33](https://github.com/Lychee-Technology/flinkt/pull/33)). An identical schema was compatible as-is, and every structural change was incompatible, for values and in every key role. Flinkt returned neither `compatibleAfterMigration` nor `compatibleWithReconfiguredSerializer`. No released state needed migrating yet, and a migration-capable serializer has to work across `restoreSerializer()`, snapshot resolution, old bytes, user-code classloaders, both state backends, savepoints and every future adapter. So the first decision committed to no migration mechanism, and kept one invariant: if the persisted schema changed, restore stops. It still required snapshots to record the full schema, never a hash, so that a later release could add migrations.

**Extension: POJO-level value evolution** ([#40](https://github.com/Lychee-Technology/flinkt/pull/40)). The project's long-term target is now parity with the schema evolution that Flink's `PojoSerializer` offers users for ordinary persisted values: remove a field, add a field with a defined value, and do both inside nested values. Without it, a job that moves from Flink POJOs to Flinkt loses the ability to drop or add a field without rewriting its state. What stays from the first decision:

- keys don't evolve structurally, in any role;
- no `compatibleWithReconfiguredSerializer`, and no key-specific snapshot class;
- every result is backed by old bytes and real restores;
- until a migration lands with its evidence, its transition is incompatible.

What changed is the value target, and with it what the first snapshot has to carry. The first decision asked that it not rule out a later migration. This one fixes the migration architecture now, so that [#5](https://github.com/Lychee-Technology/flinkt/issues/5) designs a snapshot a later reader can actually consume. The snapshot the first release writes limits what every later release can do with that state.

Parity means the same user-visible capabilities, not the same implementation. Flinkt doesn't copy `PojoSerializer`'s reflective field access, its per-record subclass tags and class names, its record format, or its internal compatibility branches. It stays generated, reflection-free on the steady-state path, and explicit about persisted identity and key safety.

## Target

### Values

This is the target for ordinary persisted values: stream records, `ValueState` values, `ListState` elements, `MapState` values, and operator and broadcast-state values.

| Change | Result |
|---|---|
| identical schema | `compatibleAsIs` |
| source declaration reorder, persisted layout unchanged | `compatibleAsIs` |
| enum constant or sealed subtype renamed, reordered or moved, ID unchanged | `compatibleAsIs` |
| field removed | `compatibleAfterMigration` |
| field added, with a defined migration default | `compatibleAfterMigration` |
| field added, without a legal migration default | `incompatible` |
| field renamed, without an explicit rename identity | a removal plus an addition |
| nested record, `List` element, `Map` value, generic argument or sealed payload migrates | `compatibleAfterMigration`, recursively |
| field type change, including nullability and type arguments | `incompatible` |
| class renamed or moved to another package | `incompatible` |
| value-class underlying type change, or a switch between a value class and its underlying type | `incompatible`, even when the bytes match |
| collection kind change | `incompatible` |
| structural change under a `Map` key | `incompatible` |
| enum constant or sealed subtype added or removed, or its ID changed | `incompatible` |

Results compose like Flink's composite serializers do. Any incompatible child makes the parent incompatible. Otherwise any child that migrates makes the parent migrate. The parent is as-is only when every child is and its own layout is unchanged. So `User.address` migrates when `Address(city, zip)` becomes `Address(city)`.

- **Reorder.** #5 orders persisted fields canonically, by name (see [What the first snapshot must carry](#what-the-first-snapshot-must-carry)). Reordering constructor parameters doesn't change the bytes, so it isn't a migration.
- **Rename.** Flinkt doesn't detect renames. `name` → `displayName` drops the old value and adds a field, which then needs a migration default or is incompatible. An explicit field ID or rename mapping would be a separate decision.
- **Added fields.** Kotlin can't leave a field unset the way Java leaves it at `null` or zero, so an added field migrates only when Flinkt can construct a legal, deterministic value for it. `null` for a nullable field, and an explicitly declared default, are the candidates. Whether any non-null type gets an intrinsic default such as `0` or `false` is [#37](https://github.com/Lychee-Technology/flinkt/issues/37)'s decision, not an assumption. A Kotlin constructor default isn't a migration contract. `val country: String = currentLocale()` can give a different value on each restore, and reaching it means calling the synthetic `$default` constructor, whose shape is a compiler detail.
- **Class identity.** A generated type is identified by its JVM class name (#5). Moving `com.old.User` to `com.new.User` is incompatible. An explicit logical type ID would be a separate decision, if relocation becomes a real need. Within a sealed hierarchy, subtypes are identified by `@FlinkId`, not by class name ([Enum and sealed identity](architecture.md#enum-and-sealed-identity)).
- **Type changes.** `Int` → `Long` is incompatible, as a `PojoSerializer` field type change is. `String` → `String?` is a type change too.

### Keys

A persisted key decides where state is filed and how it's found. A migration that reads every old key correctly can still file state under another key, or turn two old keys into one. So keys stay strict:

| Role | Identical schema | Structural change |
|---|---|---|
| value | restores | follows [Values](#values) |
| `keyBy` partition key | restores\* | fails explicitly |
| keyed `MapState` user key, heap and RocksDB | restores\* | fails explicitly |
| broadcast-state map key | restores\* | fails explicitly |
| position under a `Map` key, in any role | restores | fails explicitly |

\* The type must be eligible in that context, and its `equals()` and `hashCode()` must be unchanged ([Keys](architecture.md#keys)).

A value migration is never evidence that a key can migrate. `ValueState<User>` migrates when `User` loses `email`. A `keyBy` whose key is `User` fails with the same change.

Flink enforces part of this on its own, and not the rest ([What Flink does on restore](#what-flink-does-on-restore)). It rejects a partition-key serializer that needs migration on every keyed restore path. RocksDB requires an unchanged user key when it migrates `MapState`. The heap backend and broadcast state read every restored entry with the old snapshot's `restoreSerializer()` before any compatibility check, and afterwards accept anything but `incompatible`. On those two paths only the old snapshot can stop a key migration, so the snapshot records that its serializer was a persisted map key:

- `mapStateDescriptor<K, V>()` and `runtimeContext.mapState<K, V>()` create `K`'s serializer in the map-key role ([#13](https://github.com/Lychee-Technology/flinkt/issues/13)). A `MapStateDescriptor`'s key is a map key whether it becomes keyed `MapState` or broadcast state, so the descriptor helper can set the role without knowing which.
- The snapshot records the role ([#5](https://github.com/Lychee-Technology/flinkt/issues/5)). If the old or the current serializer is a map key, only as-is passes.
- A `Map<K, V>` inside a value needs no role. Its schema already says which position is the key.
- A `MapStateDescriptor` built by hand from `typeInfo<K>()` gives `K` the value role. It's the same exit as any descriptor passed to Flink directly, and Flink's heap backend and broadcast state would then accept a value migration of the key.

The role is data inside the one snapshot class, not a key-specific snapshot class. The first release records it even though it doesn't migrate anything yet, because a later release reading first-release state needs to know which serializers were map keys.

### Not in the target

- **`compatibleWithReconfiguredSerializer`.** `PojoSerializer` returns it when subclass registrations change. Flinkt has no dynamic subclass registration. It's added only if a concrete Flinkt serializer-configuration change needs it, and it would need proof that every key keeps its bytes, hash, key group and lookup.
- **Key migration**, in any role.
- **Explicit field IDs or rename mappings, and logical type IDs.** Each would be its own decision.
- **ID-table evolution.** Adding an enum constant or sealed subtype stays incompatible.
- **Nullability widening**, `String` → `String?`.
- **Per-record schema.** Records never carry field names, type names or schemas to make migration easier.

## Stages

Every structural change is incompatible until the stage that enables it lands with its evidence. A row of the matrix changes only in the pull request that proves it, and the README and [testing.md](testing.md#serializer-snapshot-compatibility) say which rows are enabled.

| Stage | Issues | What becomes true |
|---|---|---|
| 1. Formats | #5, #16, #18, #20, #22, implemented by #12, #15, #17, #19, #21 | snapshots can describe and consume the old layout, and every structural change is still incompatible |
| 2. Planning | [#34](https://github.com/Lychee-Technology/flinkt/issues/34) | one plan decides compatibility. No result changes |
| 3. Old-layout restore | [#35](https://github.com/Lychee-Technology/flinkt/issues/35) | `restoreSerializer()` and `resolveSchemaCompatibility` both run on the plan. No result changes |
| 4. Removal | [#36](https://github.com/Lychee-Technology/flinkt/issues/36) | removing a field from a top-level record migrates |
| 5. Addition | [#37](https://github.com/Lychee-Technology/flinkt/issues/37) | an added field with a defined default migrates |
| 6. Nested | [#38](https://github.com/Lychee-Technology/flinkt/issues/38) | removal and addition migrate inside nested records, `List` elements, `Map` values, generic arguments and sealed payloads |
| 7. Release evidence | [#26](https://github.com/Lychee-Technology/flinkt/issues/26), [#39](https://github.com/Lychee-Technology/flinkt/issues/39), [#29](https://github.com/Lychee-Technology/flinkt/issues/29), [#30](https://github.com/Lychee-Technology/flinkt/issues/30) | the matrix holds end to end: both backends, checkpoints and savepoints, a cross-adapter savepoint, released fixtures |

## Restore model

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

### What the first snapshot must carry

These are requirements on #5's format. #5 still decides the bytes.

- **A symbolic, recursive schema, never a hash.** The snapshot records every type reachable from the serialized type: record fields with their names and full types, nullability at every level, type arguments as substituted, collection shape, value-class wrapper and underlying type, and the enum and sealed ID tables with each subtype's payload schema. A type is named by a string, not resolved to a `Class`, so the schema still describes a type whose class the new job doesn't have.
- **Two versions.** A snapshot format version covers how the snapshot's own data is encoded. A record format version covers the encoding of records: scalars, strings, null bitmaps, framing. A new snapshot encoding doesn't mean the records changed, and a new record layout doesn't need a new snapshot class.
- **One stable snapshot class.** Flink writes the snapshot implementation's fully-qualified class name into checkpoint metadata and instantiates it by name on restore. That name is part of the persisted contract. It's the same on every adapter line, because a savepoint written through one adapter is restored through another (#29), and no per-line wrapper or subclass may become the class Flink records.
- **Type identity.** A generated type is identified by its JVM class name, as `Class.getName()` spells it, for example `com.example.User`.
- **Field identity by name.** A field is identified by its Kotlin property name, not by its constructor position.
- **Canonical field order.** Records write fields in an order derived from their names, so a pure declaration reorder changes no bytes. The recommended contract is ascending `String.compareTo` order of the property names, which is locale-independent and the same in every JVM. #5 confirms it with a prototype that shows generated code reading into locals in that order and then calling the constructor, so Kotlin constructor semantics don't change.
- **Compact records.** Records carry no field names, type names or schema. Metadata belongs in the snapshot.
- **Nested values inline.** A nested record is written in place, with no length prefix. A migration consumes a removed nested value by walking its old schema, so the steady-state path doesn't pay a prefix on every nested value to make a later skip cheaper.
- **The map-key role** ([Keys](#keys)).

### The planner

A Flink-free planner in `flinkt-core` compares the old and current schemas, with their roles, and returns `AsIs`, `AfterMigration(plan)` or `Incompatible(reason)` ([#34](https://github.com/Lychee-Technology/flinkt/issues/34)). For a record, the plan gives each field one operation: read it as it is, read and migrate it with a nested plan, discard a removed value, or supply an added field's default. `resolveSchemaCompatibility` maps the plan to Flink's result, and the old-layout serializer executes the same plan. A compatibility answer that the reader can't execute is therefore impossible by construction. An operation kind is enabled only in the stage that ships its evidence. Until then, the planner reports a transition that needs it as incompatible, naming the operation.

An incompatible result names the canonical type, the first incompatible difference as a path such as `com.example.User.address.zip`, the old and current schema of that node, and the role when the snapshot records one.

### The old-layout serializer

`restoreSerializer()` returns what the plan calls for ([#35](https://github.com/Lychee-Technology/flinkt/issues/35)): the current codec when the schemas are identical, and an old-layout serializer when the plan migrates. When the plan is incompatible, it returns nothing that reads the old bytes (see below). It finds the current declaration through the user-code classloader Flink passes to `readSnapshot`.

The old-layout serializer reads bytes the old serializer wrote, driven by the old schema and the plan. It reads kept fields, migrates nested values, and consumes removed values by walking their old schema, whatever their type, without loading their classes. A removed field of type `OldProfile` can be discarded after `OldProfile` has left the job's jar. The current top-level type still has to exist, because the reader constructs it.

Flink also writes with it. A state the new job doesn't register again keeps the old snapshot's serializer. The heap backend and operator state write that state with it at every later checkpoint, and both backends record its snapshot as the state's snapshot ([What Flink does on restore](#what-flink-does-on-restore)). So the old-layout serializer writes current values back in the old layout, and its snapshot is the old schema, exactly. A snapshot that described any other layout would make the next restore read the carried bytes wrongly, and Flink wouldn't notice. Added fields aren't written, and their default is supplied again on the next migration. A removed field has no value in a current object. `PojoSerializer` writes a null there, which its per-field null marker allows. A Flinkt record has no null marker for a non-null field, so [#35](https://github.com/Lychee-Technology/flinkt/issues/35) decides what it writes. The candidate is a placeholder that the old schema can read, with the snapshot marking the field's value as dropped. A later declaration that adds a field of that name then gets its migration default, not the placeholder.

Both backends call `restoreSerializer()` at restore for every restored state, registered or not. The heap backend reads with it, and RocksDB asks for it while creating column families. If it fails for an incompatible plan, the restore fails even for a state the new job no longer uses. If it instead returns a serializer that refuses to read or write but keeps the old snapshot, RocksDB can carry that state forward, and the heap backend still fails when it reads it. #5 decides which.

### Constructing current values

Kotlin constructs immutable objects only through their constructor. For every record type, KSP generates a migration constructor that takes resolved field values, knows which are present, and calls the primary constructor directly with the kept, migrated and defaulted values. It never mutates a `final` field, never uses `KProperty`, `primaryConstructor.call` or `java.lang.reflect`, and runs `init` blocks as ordinary construction does. A failure in an `init` block, or in a value class's validation, fails the restore with an error naming the type.

### Cost

The steady-state path stays as it is: generated field access, direct construction, no reflection, minimal allocation. Migration runs once per restored value, on restore for the heap backend and when the state is registered for RocksDB, and may be slower. It may allocate temporary field slots, presence bitsets and plan objects when that makes it simpler to get right. It may not reflect. The normal per-record path doesn't pay anything to make migration allocation-free.

## What Flink does on restore

Checked in the 1.20.5, 2.2.1 and 2.3.0 sources for [#40](https://github.com/Lychee-Technology/flinkt/pull/40). The files that decide these answers are identical on the three lines except where noted. The documented POJO rules are identical in the 1.20, 2.2 and 2.3 documentation ([State Schema Evolution](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/)).

- **Snapshot identity.** For each state serializer, Flink writes a format version, the snapshot class's name, the snapshot's version and its data. On restore it loads the class by name through the user-code classloader and calls its public no-arg constructor (`TypeSerializerSnapshot.writeVersionedSnapshot`, `TypeSerializerSnapshotSerializationUtil`).
- **Direction.** The runtime calls `newSnapshot.resolveSchemaCompatibility(oldSnapshot)` on every line. The older `resolveSchemaCompatibility(TypeSerializer)` is a deprecated default on 1.20 and gone on 2.x, where the new method is abstract, so implementing only the new one works on all three.
- **`restoreSerializer()` runs at restore, for every restored state.** The heap backend builds its state tables with it and reads every entry through it (`CopyOnWriteStateTable`, `StateTableByKeyGroupReaders`). RocksDB calls it while creating column families, for its TTL check (`RocksDbTtlCompactFiltersManager`), whether or not the job registers the state. Operator and broadcast state read their entries with it (`OperatorStateRestoreOperation`).
- **Values.** The heap backend rejects only `incompatible` when the state is registered again. Its values are already current objects, and the next checkpoint writes them with the new serializer. RocksDB keeps the raw bytes, from a native or incremental checkpoint and from a canonical savepoint alike. On `compatibleAfterMigration` it rewrites the state when it's registered, reading with the old snapshot's `restoreSerializer()` and writing with the new serializer, `ListState` element by element and `MapState` value by value. Operator and broadcast state reject only `incompatible`.
- **Keys.** A partition-key serializer, like a namespace serializer, that's neither as-is nor reconfigured fails the restore with `StateMigrationException` on heap checkpoints, canonical savepoints and RocksDB incremental checkpoints. RocksDB migrates `MapState` only when the user-key serializer is exactly as-is ("migration for MapState currently only allows value schema evolutions"). The heap backend's `MapState` user keys and broadcast-state keys accept a migration.
- **State the new job doesn't register.** It's carried into every later checkpoint. The heap backend and operator state write it with `restoreSerializer()`, and RocksDB copies its raw bytes. Both record `restoreSerializer().snapshotConfiguration()` as its snapshot, not the old snapshot object (`RegisteredKeyValueStateBackendMetaInfo.computeSnapshot`).
- **TTL on 2.x.** 2.x wraps state serializers and snapshots for TTL on both sides of the check. That's transparent while TTL doesn't change. When TTL is switched in the same restore as a migration, the heap backend migrates eagerly.

`PojoSerializer`, for comparison:

- **Record.** A flags byte, a subclass name or tag when the value is a subclass, then each field's null marker and bytes. Fields are in name order (`PojoTypeInfo` sorts them), so a source reorder doesn't change the bytes. No field names are written.
- **Snapshot.** The POJO's class name, then each field, identified by its declaring class and name, with its nested snapshot, then the registered and cached subclasses.
- **Resolution.** A different class, or a missing registered or cached subclass, is incompatible. So is any incompatible field, and `int` → `long` is incompatible because the nested snapshots compare serializer classes (`SimpleTypeSerializerSnapshot`). Otherwise an added or removed field, or a field that migrates, gives `compatibleAfterMigration`, recursively through `CompositeTypeSerializerUtil`. A change in subclass registrations, or any cached non-registered subclass, gives `compatibleWithReconfiguredSerializer`.
- **Restore.** `restoreSerializer()` builds a serializer for the current class with the old field list, where a removed field has no `Field`. It reads a removed value and drops it, and writes a null in its place.
- **Added fields** keep whatever the no-arg constructor and field initializers set. The documentation calls it Java's default value.
- **Removed types.** A removed field's type must still load. The snapshot read tolerates its missing class, and the compatibility check even reports a migration, but `restoreSerializer()` then fails ("field serializer snapshots should be present"). A missing top-level POJO class fails the restore.

So Flinkt's target matches `PojoSerializer` on removal, recursive migration, type changes and class identity, keeps Flink's documented rule that keys don't evolve even where Flink's backends would accept a key migration, and differs in three places. A removed value is consumed from the old schema, so its class can leave the jar. An added non-null field needs a declared migration default instead of whatever a constructor computes. And Flinkt has no subclass registration, so it doesn't need `compatibleWithReconfiguredSerializer`.

## Open decisions

- **#5:** the record bytes, within the constraints above: scalar and string encodings, the null bitmap's size, bit order and nullable-field order, the top-level nullable marker, and the snapshot's own encoding. What `restoreSerializer()` returns for an incompatible plan, as above.
- **#35:** what the old-layout serializer writes for a removed field when Flink writes a carried state with it.
- **#18:** the value-class representation. The direction is recorded in [architecture.md](architecture.md#open-questions).
- **#37:** migration defaults for added fields.
