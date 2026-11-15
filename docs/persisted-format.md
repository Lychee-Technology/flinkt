# Persisted format

This document records the bytes Flinkt persists, record format version 1 and snapshot format version 1, and what `restoreSerializer()` returns. It is the decision record for [#5](https://github.com/Lychee-Technology/flinkt/issues/5). Its direction was approved on the issue ([approved decisions](https://github.com/Lychee-Technology/flinkt/issues/5#issuecomment-6872410065)), and its field order by a later amendment, which replaced name order with constructor order and an explicit `@PersistedFieldOrder` ([superseding decision](https://github.com/Lychee-Technology/flinkt/issues/5#issuecomment-6886645867)). This document fixes the exact bytes. [schema-evolution.md](schema-evolution.md) records how a restore decides what to do with them, and [architecture.md](architecture.md#persisted-formats) records the collection, ID and key decisions built on them.

Nothing here is implemented yet. KSP generates the codecs in [#11](https://github.com/Lychee-Technology/flinkt/issues/11), the adapters' serializer and snapshot class come in [#12](https://github.com/Lychee-Technology/flinkt/issues/12), and nullable types in [#15](https://github.com/Lychee-Technology/flinkt/issues/15). The evidence is a prototype in the adapters' shared tests, [`adapters/shared/src/test/kotlin/io/flinkt/flink/format`](../adapters/shared/src/test/kotlin/io/flinkt/flink/format), which both PR lanes run, on Flink 2.3.0 and 1.20.5, and whose record and string format tests also run on 2.2.1 ([Evidence](#evidence)).

Once a release writes state in these formats, users' checkpoints and savepoints hold it, and every later release has to read it. That's why the format is decided before the first codec is generated, and why it leaves nothing to the output view, the JVM or the locale.

## Two formats, two versions

| Version | Covers | Where it's stored | Version 1 |
|---|---|---|---|
| Snapshot format version | how the snapshot's own data is encoded | by Flink, before the snapshot data: it's `getCurrentVersion()` | [Snapshot](#snapshot) |
| Record format version | how records are encoded: scalars, strings, null bitmaps, framing | in the snapshot data, as its first field | [Records](#records) |

Records carry no version. A record's format is the one its state's snapshot names, so a new snapshot encoding doesn't mean the records changed, and a new record encoding doesn't need a new snapshot class.

**Policy.** A released version is never redefined. Each release states which versions of each format it reads, tests them against released fixtures ([#39](https://github.com/Lychee-Technology/flinkt/issues/39)), and fails explicitly on any other version, naming the version it found and the versions it reads. It never guesses and never tries another serializer.

- A **snapshot format** change that an older reader would misread needs a new version. An addition that an older reader rejects by name (a new declaration or type kind, a new built-in, a [critical attribute](#attributes)) or may ignore (a non-critical attribute) doesn't need one.
- A **record format** change to the bytes of anything version 1 can express needs a new version. A new built-in kind or shape doesn't, because a schema that uses it names it, and an older reader rejects the name.
- The current codec writes only the current record format version. State in an older one needs the [old-layout serializer](schema-evolution.md#the-old-layout-serializer) (#35), so until #35 lands a record format change would make all existing state incompatible. Version 1 has to be right.

A snapshot whose record format version this release doesn't read is decoded like any other, and its restore fails, naming the version.

## Records

Record format version 1. Generated codecs write through `java.io.DataOutput` and read through `DataInput`, and mention no Flink type ([Generated code and the adapter](architecture.md#generated-code-and-the-adapter)). They use only `writeByte`, `writeShort`, `writeChar`, `writeInt` and `writeLong`, which write the same bytes on Flink's output views. `RecordFormatTest` checks it on a `DataOutputSerializer` and on a paged view (`AbstractPagedOutputView`), the two views whose floating-point writes differ, on 1.20.5, 2.2.1 and 2.3.0. `writeFloat`, `writeDouble`, `writeBytes(String)` and `writeUTF` are never used.

### Field order

A record's fields are written in its **persisted order**. By default that's the primary constructor's order. A record can set another one with `@PersistedFieldOrder` on its properties, the API the [amendment](https://github.com/Lychee-Technology/flinkt/issues/5#issuecomment-6886645867) approved:

```kotlin
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.BINARY)
annotation class PersistedFieldOrder(val order: Int)
```

- **All or none.** If one primary-constructor property of a `@FlinkType` record carries `@property:PersistedFieldOrder(order = N)`, every one does. The fields are then written in ascending order of their keys.
- **Keys are unique and non-negative, and may have gaps.** They only sort: keys `0, 10, 20` and `3, 7, 1000` give the same order, the same bytes and the same snapshot. A key isn't a field's identity, isn't written in a record and isn't recorded in the snapshot. A field is still identified by its name ([Type identity](#type-identity)).
- **KSP rejects** an annotation on some of a record's properties and not the others, a negative key, a key that two properties share, and an annotation on a property that isn't persisted: one declared in the class body, a top-level property, a property of a class that isn't a `@FlinkType`, and the property of a value class, which is written as its underlying value (#18) and has no field order. Each error is located at the declaration and names it. #11 implements the checks, with the annotation in `flinkt-core`. `PersistedFieldOrderTest`, in `flinkt-ksp`, runs a prototype processor of them.

`Account(id, name, email, balance, rating)` is written as `id`, `name`, `email`, `balance`, `rating`. With the keys `rating = 0`, `name = 10`, `email = 20`, `id = 30` and `balance = 99`, it's written as `rating`, `name`, `email`, `id`, `balance` ([Examples](#examples)). The null bitmap follows the persisted order too ([Null representation](#null-representation)).

A generated codec reads each field into a local in persisted order, then calls the primary constructor once, with each argument by name. Construction is ordinary construction: an `init` block runs once and sees every argument, so a check across fields works. `PersistedOrderTest` decodes `Key(a, b)` and the reordered `Key(b, a)`, with and without keys, compiled by Kotlin 2.4.0 and 2.4.21, and an `init` block that compares `a` with `b` passes or fails as it would for a direct call.

**The snapshot records both orders, each in its own right** ([Content](#content)): the persisted order, which is the record's layout, and the constructor order, which a data class's generated `hashCode()` follows. Neither is derived from the other, or from the names. So a restore can tell the two kinds of reorder apart:

- **Without keys, a constructor reorder reorders the layout.** `PersistedOrderTest` writes `Key(a = 1, b = 2)` as `00 00 00 01 00 00 00 02`, and after a reorder to `Key(b, a)` as `00 00 00 02 00 00 00 01`. A change of persisted order is a field permutation, which version 1 has no migration for, so it's incompatible in every role until a separately reviewed and tested permutation stage lands ([Restore](#restore)). To reorder a constructor and keep the layout, a user first gives every property a key in the old order. The planner's diagnostic says so.
- **With keys that keep the persisted order, only the constructor order changes.** The bytes stay `00 00 00 01 00 00 00 02`, but `Key(a = 1, b = 2)` hashes to 33 before and 63 after, and Flink's `KeyGroupRangeAssignment` files it under key group 29 before and 44 after, at max parallelism 128. So the change is as-is for a value and for a map key, and incompatible for a partition key, whose key groups follow `hashCode()` ([#32](https://github.com/Lychee-Technology/flinkt/issues/32)), and for a record under a `Map` key, whose keys a codec counts by hash code ([Keys that share a hash code](#resource-limits)).

Renumbering keys without changing their order changes nothing a restore sees.

Alternatives:

- **Ascending `String.compareTo` order of the property names**, which this decision first chose, kept a reorder's bytes without an annotation. It made the layout follow something the declaration doesn't show, and made every reorder's safety implicit. Constructor order is what the declaration shows, and keeping a layout across a reorder becomes an explicit choice, made with the annotation.
- **Field IDs in each record, or a header,** would let a reader find a field by ID, at a cost in every record, which the README's trade-off rules out.
- **Recording the keys in the snapshot** would make a renumbering a schema change, though no byte changes. The snapshot records the order they give.
- **Recording only one order and deriving the other** can't work either way. The persisted order can't be derived from the constructor order once keys exist, and the constructor order, which decides a partition key's key groups, can't be derived from the persisted order.

### No header

A record is the concatenation of its null bitmap, if it has one, and its fields' bytes, and nothing else. It carries no version, length, field count, type tag, field name or schema. A record without nullable fields has no bitmap, and a record without fields is zero bytes.

The cost is that a record can't show it's being misread. `RestorePrototypeTest` reads the bytes of two old `User(id)` records with the current `User(id, name)` codec and gets `User(id=1, name=Ada)`, without an error. Only the snapshot can prevent that, so a restore never uses a codec whose schema differs from the one in the snapshot ([Restore](#restore)). A header byte per record, and per nested record, would cost every state access, and would catch some misreads but not all of them.

### Scalars

| Kotlin type | Bytes | Written with |
|---|---|---|
| `Boolean` | `00` for false, `01` for true | `writeByte` |
| `Byte` | 1 byte, two's complement | `writeByte` |
| `Short` | 2 bytes, big-endian | `writeShort` |
| `Char` | 2 bytes, big-endian: the UTF-16 code unit | `writeChar` |
| `Int` | 4 bytes, big-endian | `writeInt` |
| `Long` | 8 bytes, big-endian | `writeLong` |
| `Float` | 4 bytes, big-endian, of `floatToIntBits(v)`: every NaN is `7f c0 00 00` | `writeInt` |
| `Double` | 8 bytes, big-endian, of `doubleToLongBits(v)`: every NaN is `7f f8 00 00 00 00 00 00` | `writeLong` |
| `String` | [Strings](#strings) | `writeByte` |

Fixed widths keep generated code, fixtures and debugging simple, and nothing measured says a variable-length integer would pay for its edge cases. That matches the collection sizes of [#16](architecture.md#collections) and the IDs of [#20](architecture.md#enum-and-sealed-identity). Signed zero isn't normalized: `-0.0` is `80 00 00 00 00 00 00 00`, and a data class compares `-0.0` and `0.0` as unequal too.

Each field has the bytes that Flink's own serializer for that type writes on a `DataOutputSerializer`: `BooleanSerializer` through `StringSerializer` (`RecordFormatTest`, on both lines). That's deliberate. A `String` then has one encoding whether it's a top-level type, whose bytes are Flink's ([Type information](architecture.md#type-information)), or a field, whose bytes are Flinkt's. There are two exceptions. On a paged view, Flink's `FloatSerializer` and `DoubleSerializer` keep a NaN's payload and Flinkt's format doesn't. And Flinkt's reads are stricter ([Decoding](#decoding)).

### Strings

A `String` is written in the char-varint format of Flink's `StringValue.writeString`, which this decision specifies as Flinkt's own:

1. the length in UTF-16 chars, plus one, as an unsigned varint;
2. each UTF-16 char, as an unsigned varint.

A varint is 7 bits per byte, least significant group first, with the high bit set on every byte but the last. So a char takes one byte up to U+007F, two up to U+3FFF and three up to U+FFFF. A surrogate takes three, so a supplementary character such as `😀` takes six, where UTF-8 takes four. An unpaired surrogate is just a char. The length's plus one is Flink's, which writes `00` for a null string. Flinkt writes null in a bitmap or a marker, so a length byte of `00` is malformed.

```text
""          01
"Ada"       04 41 64 61
"é"         02 e9 01                 U+00E9
"中"        02 ad 9c 01              U+4E2D
"😀"        03 bd b0 03 80 bc 03     U+D83D U+DE00
"\uD800"    02 80 b0 03              an unpaired high surrogate
"a" × 127   80 01 61 61 …            the length plus one, 128, takes two bytes
```

Generated code doesn't call Flink to write it. `StringFormatTest` compares Flinkt's bytes with `StringValue.writeString`'s over fixed cases, unpaired surrogates included, every length where the length's varint grows, a 70,000-char string and 500 random strings of arbitrary UTF-16 code units, on both views, on 1.20.5, 2.2.1 and 2.3.0. Each reads what the other wrote.

The format is lossless for every `String`, so two different strings never have the same bytes, which key determinism needs ([Deterministic bytes](#deterministic-bytes)). A string has at most 67,108,864 chars, 2^26, a budget the writer enforces ([Resource limits](#resource-limits)). The format itself would take up to `Int.MAX_VALUE - 1`. Alternatives:

- **UTF-8 through `String.toByteArray`** writes an unpaired surrogate as `?`, the same byte as `"?"`. `StringFormatTest` checks it.
- **`writeUTF`** fails on an encoding over 65,535 bytes.
- **Two bytes per char** would double every ASCII string and gain nothing.
- **A lossless UTF-8 variant** (CESU-8 or WTF-8) would need an encoder of its own. A field would then have other bytes than the same string at the top level, where Flink's `StringSerializer` writes it.

The choice doesn't rest on speed, and nothing was measured. If #12 finds the per-char writes slow, an encoder can buffer them and still write the same bytes. A different encoding would be a new record format version.

### Null representation

**The null bitmap.** A record with nullable fields starts with a bitmap, before any field:

- one bit per nullable field, in persisted order restricted to the nullable fields;
- `ceil(n / 8)` bytes for `n` nullable fields;
- bit `k` is in byte `k / 8`, as the value `1 shl (k % 8)`: least significant bit first;
- `1` means null and `0` present. A null field has no bytes after the bitmap;
- unused bits in the last byte are `0`. A set unused bit is malformed;
- no bitmap at all when the record has no nullable field.

Which fields are nullable is decided by the declaration: a field whose declared type is marked nullable, such as `String?`, `User?` or `T?`. A field declared as a bare type parameter `T` isn't in the bitmap, even when its argument is nullable. Its value is written by the argument's codec, which for a nullable argument writes a marker. A present `T?` field is written by its argument's codec too, so with a nullable argument it has a bitmap bit and then a `01` marker. So a record's layout, bitmap included, depends only on its declaration, never on its type arguments. Whether generic classes are supported is [#22](https://github.com/Lychee-Technology/flinkt/issues/22)'s decision, and if they are, these are their bytes.

A null `T?` field is its set bit, and nothing else. So after a clear bit, a nullable argument's marker is `01`, and every reader, a generated codec and the old-layout serializer alike, fails on `00` there. Accepting it would give a null field a second encoding, which no writer produces. `Box<T>(value: T, previous: T?)`, persisted order `value`, `previous`, with `T` = `User?`:

```text
Box(value = User(1, "a"), previous = User(2, "b"))    00 | 01 <User(1, "a")> | 01 <User(2, "b")>
Box(value = null, previous = null)                    01 | 00
00 | 00 | 00                                          malformed: previous's bit is clear, so its marker is 01
```

**The null marker.** A nullable value anywhere else is one marker byte, then the non-null type's bytes when it isn't null: `00` for null, `01` before a value. Any other marker byte is malformed. That covers a top-level type such as `typeInfo<User?>()`, a `List` element, a `Map` key or value, and the argument of a field declared as `T`. A top-level nullable built-in, such as `typeInfo<String?>()`, wraps Flink's own serializer, so the bytes after `01` are Flink's ([Nested Flink snapshots](#nested-flink-snapshots)).

`PojoSerializer` writes a null marker per field. A bitmap takes one byte per eight nullable fields, and none when there are none. With `1` meaning null, a record whose nullable fields are all present has a bitmap of zeros.

This is the serializer's representation only. Flink's backends treat `ValueState.update(null)` as `clear()` and write their own null flag for `MapState` values. Flinkt keeps both (#15).

### Nested values

A nested record is written inline, in its own format, bitmap included, with no length prefix. So is a nested enum, sealed value or collection, in the formats [architecture.md](architecture.md#persisted-formats) gives them. A migration that removes a nested value consumes it by walking its old schema ([Restore](#restore)). So the steady-state path pays no prefix on every nested value to make a later skip cheaper.

### Deterministic bytes

The same value has the same bytes on every view, in every JVM and every locale, except for a `Map`, whose entries are written in iteration order ([#16](architecture.md#collections)).

For a type that's eligible as a key ([#23](architecture.md#eligibility)), equal values have identical bytes and unequal values different bytes. Each eligible component's encoding agrees with data-class equality. A data class compares a `Float` or `Double` by `floatToIntBits` or `doubleToLongBits`, which is what's written, and a `String` by its UTF-16 code units, which are all written. Each encoding is also self-delimiting, fixed-width or length-first, so a record that concatenates them in a fixed order is too. `RecordFormatTest` checks both directions over every pair of edge values: NaNs with different payloads, signed zeros, surrogates, prefixes, and composite keys.

Byte equality says nothing about a key's hash code or key group. Those depend on the compiler that built the key class and on constructor order, and [#32](https://github.com/Lychee-Technology/flinkt/issues/32) pins them. A key's bytes are not evidence of compatibility either: `UserId(1)` and `1L` have the same bytes under #18's direction, and are different schemas.

### Decoding

Readers are strict. Bytes that no value encodes fail with an error naming what's wrong, never decode as a value that would be written differently, and never fall back to another serializer:

- a `Boolean` or null marker other than `00` or `01`, and a `00` marker after the clear bitmap bit of a field declared `T?`;
- a set unused bitmap bit;
- a `Float` or `Double` NaN other than the canonical one;
- a string length of `00`, a varint with a redundant last `00` byte, a length varint over 5 bytes or `Int.MAX_VALUE`, a char varint over 3 bytes or U+FFFF;
- an enum or sealed ID the type doesn't have ([#20](architecture.md#enum-and-sealed-identity)), a negative collection size ([#16](architecture.md#collections)), or a `Map` that holds a key twice;
- a string or collection over its budget, or a `Map` with more keys of one hash code than its budget ([Resource limits](#resource-limits));
- truncated input, as `EOFException`.

Each is one comparison on a path the codec takes anyway. Flink's own readers accept some of these, such as a non-canonical NaN or a non-minimal varint. Nothing Flinkt writes produces them.

### Resource limits

A corrupt or hostile input must fail with an error, not exhaust memory, stack or time. Structural checks, above, are always enforced. Resource budgets are limits on legitimate values too, so each one is enforced where the value is written as well as where it's read: a release never writes what it would refuse to read, and a checkpoint it completed stays readable.

**Threat model.** The limits below defend against one attacker, so a finding can be judged against it:

- **The bytes are untrusted.** A record, and the snapshot beside it, may be corrupt or written by an attacker, who can choose every byte, including which declared enum constants, objects and subtypes a value names.
- **The declarations are not.** The current classpath's declarations are compiled into the job by its developers. Someone who can change them can already run code in the job, which no format check prevents. A snapshot's old schema is bytes, so it's untrusted, and the snapshot reader checks it ([Validation and budgets](#validation-and-budgets)).
- **The guarantee.** Reading fails with an error or returns a value. It never exhausts the stack, allocates at most a constant factor of the bytes it consumed, and its work is linear in those bytes, apart from a `Map`'s keys, each of which is compared with a number of others that the declarations and the budgets bound ([What a value can cost](#resource-limits)).
- **The bound doesn't depend on how names hash.** It holds for every declaration the rules accept, names chosen to collide included. The limit on a key type's objects is what makes that so. The model doesn't require it, but it's checked once per serializer and costs nothing per record, so the bound can be stated without assuming the names are benign.
- **Out of scope:** what the job's own code does with a decoded value (a `LinkedHashMap` it fills with the same keys pays the same comparisons), Flink's own serializers, the state backends, and volume: a value is bounded by what Flink hands the serializer, not by a Flinkt budget.

So a new limit is needed only for an input inside this model: bytes an attacker writes against declarations the rules accept, which break a bound this section states. A slower read that stays within the stated bound isn't a defect.

| Limit | Version 1 | Written | Read |
|---|---|---|---|
| a `String`'s length | 67,108,864 chars, 2^26 | `serialize` fails before writing it | fails at the length, before reading a char |
| a `List`'s or `Map`'s size | 67,108,864 elements or entries, 2^26 | `serialize` fails before writing the size | fails at the size, before reading an element |
| a `Map`'s keys that share one `hashCode()`, counting keys that are records or value classes | 1,024 keys, 2^10 | `serialize` fails before writing the size | fails at the key that's one too many |
| the objects a `Map` key type holds, as the key type or as a sealed key type's subtypes at any level | 1,024 objects, 2^10 | the serializer can't be created | the serializer can't be created, so no map of them is read |
| nesting | 64 levels ([Validation and budgets](#validation-and-budgets)) | the serializer can't be created | the snapshot is rejected |
| a type that's written as no bytes | only the serialized type or a sealed subtype | the serializer can't be created | the snapshot is rejected |

The budgets are judgment, not measurement: far beyond what one state value should hold, and small enough that what one corrupt length can claim is bounded. A later release may raise them, never lower them.

**No zero-width elements.** An object, and a record without fields, are written as no bytes. Either one may be the serialized type, or a sealed subtype, whose ID comes first. It can't be a field, a `List` element, a `Map` key or value, or a type argument, with or without `?`. Every other type takes at least a byte: a scalar, a string's length, a collection's size, an enum or sealed ID, a null marker, or a record's bitmap or a field. So every turn of a loop over a collection consumes input, and so does every value a reader visits, except along a chain of nested records, which the nesting budget bounds. Without the rule, a `List<Shutdown>` whose size is corrupt would be four bytes that keep a reader looping, and allocating, for up to 2^26 elements without consuming input, and a schema of records that each hold two of the next, 64 levels deep, would make one value of zero bytes cost about 2^64 visits. The rule is part of the [validation](#validation-and-budgets) the writer and the reader share, so it costs nothing per record. Such a type has one value, so a field of it holds no information.

**What a reader allocates.** Nothing is allocated from a count. A string reader starts with at most 8,192 chars and doubles as it reads, and a collection reader starts with room for at most 1,024 elements and grows as they're read. So a corrupt length or size within its budget fails at the end of the input, having allocated for what it read. Each element and char takes at least a byte, so a decoded value holds at most a constant factor of the bytes it was read from. Flink's `StringValue.readString` allocates the claimed length, when it's over 2,048, before it reads a char.

**Keys that share a hash code.** A codec decodes a `Map` into a `LinkedHashMap` (#16), which finds an earlier equal key through `hashCode()`. `HashMap` keeps the keys of one hash code in one bin, and once a bin holds more than 8 it orders them by `compareTo` if their class is `Comparable` to itself (`HashMap.comparableClassFor`). `String` and the boxed built-ins are. A data class or a value class isn't, so `HashMap` compares a key of one with every earlier key of its hash code. Without a budget, a corrupt or hostile map whose keys share a hash code costs comparisons that grow with the square of its size. 16,000 keys `Key(i, -31 * i)` of a data class with two `Int` properties all hash to 0. With a `Boolean` value each they're 144,004 bytes, and reading them made 128 million `equals()` calls, 1.5 seconds on JDK 17. 4,000 and 8,000 keys made 8 and 32 million (`ResourceLimitsTest`, with keys that count their calls; the exact counts below are from JDK 17).

So version 1 has a budget: in a `Map` whose key type is a record, a value class or a sealed type, at most 1,024 keys that are records or value classes share one `hashCode()`.

- The reader fails at the key that's one too many, so it compares each key with at most 1,024 keys of its hash code, the uncounted ones below, and a logarithmic number of others. The 16,000 keys above fail after 1,025 keys and 533,801 calls. 16 hash codes with 1,024 keys each are read with 8.5 million, about 520 per key.
- The writer fails before it writes the size, so a release never writes a map it would refuse to read.
- A built-in key type isn't counted: `HashMap` orders its keys. 2^14 keys that share a hash code and are `Comparable` to themselves were read with 753,786 `equals()` and `compareTo()` calls, 46 per key.
- An enum constant isn't counted, as the key type or in a sealed key type. Its `hashCode()` is `java.lang.Enum`'s, which is final and an identity hash code: the JVM picks it, and neither a declaration nor the bytes can make two constants share one.
- An object isn't counted either, as the key type or in a sealed key type, and a map holds each at most once. That alone doesn't bound what objects cost. A `data object`'s generated `hashCode()` is its qualified name's ([Enum and sealed identity](architecture.md#enum-and-sealed-identity)), and `"Aa"` and `"BB"` hash alike, so `Signal.AaAa` and `Signal.BBBB` share a hash code, and so do 8,192 data objects named with 13 pairs of those letters. A map holds each of them once, and its read compares each with every earlier one: through keys that hash as they do and count their `equals()` calls, maps of 2,048, 4,096 and 8,192 of them made 2.1, 8.4 and 33.6 million calls. So a `Map` key type holds at most 1,024 objects, as the key type or as subtypes of a sealed key type at any level, and the serializer of one that holds more can't be created. Then a key is compared with at most 1,024 objects. 1,024 objects and 1,024 records under one hash code, the budgets' worst case, were read with 2.1 million calls, about 1,034 per key.
- The limit counts plain objects too, though their hash codes are identity hash codes, so adding `data` to an object never makes a key type invalid. It's checked with the other key rules, once per declaration, and depends on no hash code, so the writer and the reader agree on it in any JVM. Like the other budgets it's judgment, far beyond the objects a sealed key type should hold, and a later release may raise it, never lower it.
- Counting costs nothing for a map of up to 1,024 keys, because no hash code can be over the budget. Past that, a codec computes each counted key's `hashCode()` once more, and counts it in a table keyed by boxed `Int`s. `HashMap` orders those by `compareTo`, so the table's own lookups stay logarithmic however the hash codes collide, and it holds an entry per distinct hash code, a constant factor of the map.

The writer counts with the hash codes of the JVM that writes the map, and the reader with those of the JVM that reads it, so a key's hash code has to be the same in both. A record's or value class's generated `hashCode()` combines its components' hash codes, which Java SE specifies for the built-ins, with a formula that every supported Kotlin compiler shares ([Key identity across compiler versions](kotlin-compatibility.md#key-identity-across-compiler-versions)). An enum constant's or a plain object's hash code is an identity hash code, which depends on what the JVM did before ([testing.md](testing.md#comparator-and-key-semantics)): `Tagged(color: Color, n: Int)` hashes to `31 * System.identityHashCode(color) + n`. So a `Map` key type can't hold an enum, or a sealed type with an object subtype, inside a record or a value class ([Map keys](architecture.md#map-keys)). An enum constant or an object can still be the key itself, as the key type or a subtype of a sealed key type, where it isn't counted, and a key type holds at most 1,024 objects. The prototype's `MapKeys` decides which keys a codec counts, and rejects the rest, from the key type when the codec is created.

**The budget holds across every as-is change.** A map that a release wrote within the budget has to stay readable after any change a restore accepts as-is. So no as-is change may alter a counted key's hash code. The components' hash codes are specified, and the formula is pinned across compilers, but a data class combines them in constructor order. A constructor reorder of a record under a `Map` key, persisted order kept, would turn 1,025 keys `Key(-31 * i, i)`, whose hash codes `31 * a + b` are all different, into keys whose hash codes `31 * b + a` are all 0. The old release wrote that map, 9,229 bytes, and the current codec fails its read at the 1,025th key. No reader can tell such a map from a hostile one: both have 1,025 keys of one hash code, and building the `LinkedHashMap` costs the same for either. So that reorder isn't as-is. The planner finds every record a `Map` key type holds, as the key or at any depth inside it, and fails the restore on a constructor reorder of one, before anything is read ([Restore](#restore)). In a value outside a map key, and in the map-key role, the same reorder stays as-is, because nothing there is counted: the heap backend and broadcast state rebuild their own maps with the restoring JVM's hash codes, and RocksDB finds an entry by its bytes.

**The key check is linear in the schema.** Which keys a codec counts, and which key types it rejects, is decided from the key type, which may be generic: `D01<Long>`, whose declarations each hold three instantiations of the next, holds 3^30 instantiations at 31 levels. `MapKeys` never substitutes an argument into a declaration. It checks each declaration once, as a function of its type arguments: what it holds whatever they are, and which type parameters' arguments are components of it, with a path to each. A use of the declaration then checks only those arguments, where they're written. So the work and what it allocates are linear in the schema's size, as the snapshot's nesting check is ([Validation and budgets](#validation-and-budgets)), and a failure still names the path to the enum or object it found.

The budget is judgment, not measurement. A dense grid of `Key(x, y)` over 8,192 × 8,192 points, the size budget, puts at most 265 keys under one hash code, and 1,024 leaves a margin above that. As with the other budgets, a later release may raise it, never lower it.

**What a value can cost.** The work of reading a value is linear in the bytes it consumes, at most 64 levels deep, apart from a `Map`'s keys, and those are bounded: each is compared with at most 1,024 counted keys of its hash code, at most 1,024 objects, any enum constant or `null` that shares its hash code, and a logarithmic number of others. No budget caps a whole value's size: that would take a counter on every read of every record, a cost the generated codecs don't pay. A value is bounded by what Flink hands the serializer instead: one value's bytes on RocksDB, a state's checkpoint stream on the heap backend. Within that, a corrupt length claims at most its budget.

**Where each limit applies.**

- A generated codec checks each string's length and each collection's size, as it writes and as it reads, one comparison each. It counts a map's keys by hash code once the map has more than 1,024. Nesting, the zero-width rule, which keys are counted and the limit on a key type's objects are decided once, when its serializer is created, not per record, in time linear in the schema.
- The snapshot reader applies the [snapshot budgets](#validation-and-budgets), and the same nesting and zero-width checks to the old schema, before a restore uses it.
- The old-layout serializer (#35) walks the old schema that the snapshot reader accepted, with the codecs' checks and budgets. It prepares each declaration once, so no value costs work for its schema's width: it looks a type argument up in the scope where it was written instead of substituting it, and finds an enum or sealed ID by binary search. So its work is linear in the bytes it consumes too, apart from a removed `Map`'s keys, below.
- To reject a removed `Map` that holds a key twice, as a codec does, the old-layout serializer compares keys by their bytes, because it has no key class. That's exact: a key's equality depends only on what's written ([Map keys](architecture.md#map-keys)), and each encoding is canonical, so two keys are equal exactly when their bytes are ([Deterministic bytes](#deterministic-bytes)). A key that holds a `List` or a `Map` fails, because no key type holds one, and a `Map`'s bytes follow its iteration order. The keys' bytes are the only thing it keeps for a value it consumes, until the map ends, in a set ordered by those bytes. That's a constant factor of the bytes it consumed, as a codec's decoded map is. Each key costs at most `log2` of the map's size in comparisons, 26 at the budget, whatever the keys are, because the set uses no hash code that keys could be written to share. Without the key class it can't count a removed map's keys by hash code, and it doesn't need to, so it consumes a removed map that a codec would refuse for its hash codes, which no release wrote. Nor does it need the rule on enums and objects inside a key's records, or the limit on a key type's objects, which are about hash codes: their bytes are canonical too, so it compares those keys exactly. A `Map` it builds as part of a current value is built as a codec builds one, budget included.
- Flink's own framing around a Flinkt value is Flink's, and outside these limits. A `ListState`'s list, for instance, is read by Flink's `ListSerializer` where a backend reads it as one value, and that allocates for the count it reads (checked in 2.3.0).

`ResourceLimitsTest` covers each: a string over its budget fails at its length though every char follows, a collection at its size, and neither is written; a map with 1,025 keys of one hash code fails at the last of them and isn't written, the read's comparisons are those above, and `MapKeys` counts and rejects the key types above, checks `D01<Long>`'s 3^30 instantiations allocating under 1 MiB, and finds the records a codec counts, generic ones included; eight data objects whose names share a hash code share one, `MapKeys` rejects a key type of 1,025 objects, at one level or across two, and of the 8,192 above, and keys that stand for objects make the comparisons above, with and without the limit; corrupt sizes within the budget, nested ones included, allocate under 1 MB before the end of the input; `List<Shutdown>`, a `Map` value, a field and a type argument without bytes are refused by the writer, and `List<Shutdown>` by the reader; the old-layout walker consumes a byte on every turn of a corrupt `List<Long?>`, allocates nothing, and stops at the budget; a corrupt `Map<Int, Long?>` makes it keep under 64 times the bytes it consumed; and it finds a key that's there twice after 65,536 others, as `RecordFormat.readMap` does. `SnapshotFormatTest` checks that it fails on the bytes `RecordFormat.readMap` fails on, and on keys that are null, have a bitmap, a type argument or an enum, or hold a collection.

### Where the codecs live

Generated code writes a built-in field itself, in these formats. A Flinkt codec for a built-in is needed only where one is composed, as a `List<String>` element (#17) or an `Envelope<String>` argument (#22). Those codecs, and the format primitives generated code calls, live in `flinkt-core`, which has no Flink dependency. They aren't registered through the codec SPI, which still has no way for a module to register a built-in's codec, so no jar on the classpath can redefine the bytes of `Int` or `String`. #17 or #22, whichever comes first, adds them. A top-level built-in stays Flink's own `TypeInformation` and serializer (#7). The prototype's primitives are `RecordFormat`, in test code, and `MapKeys` decides which of a map's keys a codec counts.

### Examples

Each example is written through a `DataOutputSerializer` and through a paged view, with the same bytes on both. `RecordFormatTest` asserts the bytes and the round trip, and #12 and #15 turn them into golden fixtures.

```text
User(id: Long, name: String)

User(42, "Ada")
00 00 00 00 00 00 00 2a     id
04 41 64 61                 name
```

The nullable variant, in constructor order:

```text
Account(id: Long, name: String?, email: String?, balance: Double?, rating: Float)
persisted order: id, name, email, balance, rating. Bitmap: name bit 0, email bit 1, balance bit 2.

Account(7, "Ada", null, Double.fromBits(0x7ff0000000000001), Float.fromBits(0x7f800001))
02                          bitmap: email is null
00 00 00 00 00 00 00 07     id
04 41 64 61                 name
7f f8 00 00 00 00 00 00     balance: a NaN with a payload, written as the canonical NaN
7f c0 00 00                 rating: the same for Float

Account(7, null, null, null, -0.0f)
07                          bitmap: name, email and balance are null
00 00 00 00 00 00 00 07     id
80 00 00 00                 rating: negative zero
```

The same record with `@PersistedFieldOrder`, whose sparse keys give it another persisted order, the bitmap's included:

```text
Account(@property:PersistedFieldOrder(order = 30) id: Long,
        @property:PersistedFieldOrder(order = 10) name: String?,
        @property:PersistedFieldOrder(order = 20) email: String?,
        @property:PersistedFieldOrder(order = 99) balance: Double?,
        @property:PersistedFieldOrder(order = 0) rating: Float)
persisted order: rating, name, email, id, balance. Bitmap: name bit 0, email bit 1, balance bit 2.

Account(7, "Ada", null, Double.fromBits(0x7ff0000000000001), Float.fromBits(0x7f800001))
02                          bitmap: email is null
7f c0 00 00                 rating
04 41 64 61                 name
00 00 00 00 00 00 00 07     id
7f f8 00 00 00 00 00 00     balance

Account(7, null, "e", null, -0.0f)
05                          bitmap: name and balance are null
80 00 00 00                 rating
02 65                       email
00 00 00 00 00 00 00 07     id
```

```text
Session(user: User, note: String?, previous: User?)
persisted order: user, note, previous. Bitmap: note bit 0, previous bit 1.

Session(User(42, "Ada"), "hi", User(41, "Al"))
00 | 00 00 00 00 00 00 00 2a 04 41 64 61 | 03 68 69 | 00 00 00 00 00 00 00 29 03 41 6c

Session(User(42, "Ada"), null, null)
03 | 00 00 00 00 00 00 00 2a 04 41 64 61

Wide(i, h, g, f, e, d, c, b, a: Boolean?), nine nullable fields: two bitmap bytes, i bit 0 to a bit 8
all null              ff 01
only a = true         ff 00 | 01
only i = false        fe 01 | 00

Top-level User?
null                  00
User(42, "Ada")       01 | 00 00 00 00 00 00 00 2a 04 41 64 61
```

`RecordFormatTest` also has a record with a field of every scalar kind at its boundary values.

## Snapshot

Snapshot format version 1.

### The snapshot class

Flink writes the snapshot class's fully qualified name into every checkpoint and savepoint, and creates it by name on restore, through the user-code class loader, with its public no-argument constructor (checked in 1.20.5, 2.2.1 and 2.3.0). That name is part of the persisted contract:

- Flinkt has **one** snapshot class, **`io.flinkt.flink.FlinktSerializerSnapshot`**, with a public no-argument constructor. Every generated type, every key role and every adapter line uses it. Roles and schemas are data in it, never subclasses.
- Every adapter compiles it from the shared source, so the name is the same on each line, as [#29](https://github.com/Lychee-Technology/flinkt/issues/29)'s cross-adapter savepoint needs. No per-line subclass or wrapper may become the class Flink records.
- It's never renamed or moved. A job that relocates `io.flinkt` packages when it shades its jar changes the name Flink records, and has to keep that relocation for as long as it restores that state.

The prototype's class is `PrototypeSerializerSnapshot`, which `RestorePrototypeTest` restores through Flink's own `TypeSerializerSnapshot.readVersionedSnapshot`.

### Content

The snapshot data is the Flink-free part, which `flinkt-core` encodes, and then the adapter's part. The Flink-free part holds:

- the **record format version**;
- the **key role**: value, map key or partition key ([Keys](schema-evolution.md#keys));
- the **root type**: the serialized type, with its type arguments as substituted, such as `com.example.Envelope<com.example.User>`;
- every **declaration** reachable from it, each once, sorted by type name:
  - a record: its type parameters, its fields in **persisted order**, each with its name and full type, and its **constructor order**, as each field's position in that list;
  - a value class: its type parameters and its underlying property;
  - an enum: each constant's ID and name, in ascending ID order;
  - a sealed type: its type parameters, and each direct subtype's ID and type, in ascending ID order;
  - an object: its name.

A type is a class type, with its name, its nullability and its arguments with their variance, or a reference to a type parameter of the declaration it's in, with its nullability. That's the core schema model (`DeclarationSchema` and `SchemaType`), so a decoded snapshot is the model a planner compares. Types are named by strings, never resolved to a `Class`, so a snapshot describes a type whose class the job no longer has.

How this meets [What the first snapshot must carry](schema-evolution.md#what-the-first-snapshot-must-carry):

| Requirement | Where |
|---|---|
| a symbolic, recursive schema, never a hash | the root type and the declarations |
| record fields with names and full types, nullability at every level, type arguments as substituted | fields' types, and the root type's arguments |
| collection shape | `kotlin.collections.List` and `Map` class types with their arguments |
| value-class wrapper and underlying schema (#18) | the value-class declaration |
| enum and sealed ID tables, with each subtype's payload schema (#20) | the enum and sealed declarations, and each subtype's own declaration |
| two versions | Flink's snapshot version, and the record format version in the data |
| one stable snapshot class | [The snapshot class](#the-snapshot-class) |
| type identity by JVM class name | declaration names ([Type identity](#type-identity)) |
| field identity by name, the persisted order | field names, in the order a record's bytes follow |
| constructor order | a record's constructor order, beside its fields |
| the key role | the role byte |

Enum constants and sealed subtypes are written in ID order, so declaring them in another order changes neither the records nor the snapshot. A record's fields are written in persisted order, which is its layout, and its constructor order follows as a list of positions in it. Each is recorded as it is, never derived from the other or from the names, and `@PersistedFieldOrder`'s keys aren't recorded, only the order they give. So a constructor reorder without keys changes the fields' list, one with keys that keep the persisted order changes only the positions, and a renumbering changes nothing. `SnapshotFormatTest` checks each, and that both orders round-trip.

A declaration is referenced by its type name. That name is its identity, and it appears once in the snapshot, so a type reached from several fields is written once. A recursive type, `Node(children: List<Node>)`, would need no new format. The first release rejects one, as the codec SPI does.

### Encoding

Integers are big-endian `Int`s, written with `writeInt`, except where a byte is noted. Strings are in the [string format](#strings), which is part of snapshot format version 1 whatever the record format version. A `Boolean` is `00` or `01`.

```text
snapshot data, version 1
    Int          record format version
    byte         key role: 0 value, 1 map key, 2 partition key
    Type         root type
    Int          declaration count
    Declaration  …, in ascending String.compareTo order of their names
    Attributes   snapshot attributes
    Int          adapter part: the number of nested Flink snapshots
    …            each written with Flink's TypeSerializerSnapshot.writeVersionedSnapshot

Type
    byte 1       class type: String name, Boolean nullable, Int argument count, then each argument:
                     byte variance: 1 invariant, 2 in, 3 out (0, the star projection, is reserved)
                     Type
    byte 2       type parameter: String name, Boolean nullable

Declaration
    byte kind, String type name, then by kind:
    1 record       Int type-parameter count, String each; Int field count, Field each, in persisted order;
                   Int field count again, then each field's Int position in that list, in constructor order
    2 value class  Int type-parameter count, String each; Field underlying
    3 enum         Int constant count; Int ID, String name each, in ascending ID order
    4 sealed       Int type-parameter count, String each; Int subtype count; Int ID, Type each, in ascending ID order
    5 object       nothing
    Attributes     declaration attributes

Field
    String name, Type type, Attributes field attributes

Attributes
    Int count, then each in ascending tag order: Int tag, Boolean critical, Int length, the length's bytes
```

Built-ins (`kotlin.Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Char`, `String`) and collections (`kotlin.collections.List` and `Map`) are named by their Kotlin names and have no declaration. Every other class type names a declaration in the snapshot.

`User(id: Long, name: String)` in the value role is 132 bytes of data:

```text
00 00 00 01                     record format version 1
00                              role: value
01 11 "com.example.User" 00 00 00 00 00
                                root: class type com.example.User, not nullable, no arguments
00 00 00 01                     one declaration
01 11 "com.example.User"        a record, com.example.User
00 00 00 00                     no type parameters
00 00 00 02                     two fields, in persisted order
03 "id"    01 0c "kotlin.Long"   00 00 00 00 00    00 00 00 00
05 "name"  01 0e "kotlin.String" 00 00 00 00 00    00 00 00 00
                                each: name, type, no attributes
00 00 00 02 00 00 00 00 00 00 00 01
                                constructor order: id is field 0, name field 1
00 00 00 00                     no declaration attributes
00 00 00 00                     no snapshot attributes
```

`SnapshotFormatTest` has these bytes in full. A user's checkpoint holds them after Flink's framing: the class name with `writeUTF`, then the snapshot format version as an `Int`, then the data, then the adapter's part, here `00 00 00 00`.

Fixed-width integers make a snapshot larger than varints would: with one-byte varints, `User`'s would be 90 bytes instead of 132. A snapshot is written once per state per checkpoint, not per record. Fixed widths keep it readable as hex and match the record format.

### Nested Flink snapshots

A Flinkt serializer delegates to a Flink serializer in one place in version 1: a top-level nullable built-in, such as `typeInfo<String?>()`, wraps Flink's `StringSerializer`. Its bytes after the marker are Flink's, so Flink's snapshot of that serializer decides their compatibility. The adapter's part of the snapshot nests it, written and read with Flink's own versioned snapshot methods. There's exactly one when the root type is a nullable built-in, and none otherwise. A restore is as-is only when the schemas are identical and Flink resolves the nested snapshot as-is. #15 implements it, and the prototype writes the empty case.

### Validation and budgets

The writer and the reader run the same checks, so a release never writes a snapshot it would reject, and never rejects one it could have written:

- each declaration appears once, in ascending name order;
- every class type is a built-in, a collection or a declared type, with the number of type arguments it takes, and no star projection;
- a type parameter is referenced only inside a declaration that declares it, and the root type references none;
- every declaration is reachable from the root, and none reaches itself. The first release supports no recursive type;
- an object or a record without fields is only the root type or a sealed subtype ([Resource limits](#resource-limits));
- a record's field names are unique, and its constructor order names each of its fields once. Enum and sealed IDs are positive and ascending. A sealed subtype is a declared record, object or sealed type;
- names aren't empty;
- an unknown kind, variance, role or critical attribute fails, naming it.

Corrupt input is also kept from exhausting memory or stack:

| Budget | Version 1 | Why this number |
|---|---|---|
| snapshot data | 16 MiB | far beyond any schema we expect, and small enough to hold in memory |
| a name | 65,535 chars | a JVM class file can't hold a longer class, field or type-parameter name |
| nesting | 64 levels | how deep reading a value recurses, in a codec or in the old-layout serializer walking a removed value: one level per record, collection, value class or sealed type, through type arguments as substituted |

No list is allocated from a count. Each element is read before it's added, and each takes at least a byte, so the size budget bounds every list. A name over its budget fails at its length, before its chars are read. The writer enforces the same budgets when a serializer is created, before anything is persisted. A legitimate checkpoint therefore never exceeds them. The numbers are judgment, not measurement. A later release may raise a budget, but never lower one.

The checks themselves are bounded too. Declarations are found and checked one at a time, not by recursion, and one walk from the root checks reachability, cycles and nesting at once. It stops at 64 levels, so it never recurses deeper, however long a chain of declarations the size budget admits. It computes each declaration's nesting once, as a function of its type arguments' nesting, so a generic schema costs linear work however many instantiations a value can hold. A cycle shorter than the budget fails as recursive, and a longer one as too deep.

Building the schema the checks run on is linear as well, however wide a declaration is. The schema model checks each use of a type parameter against a set of the declaration's type parameters, not a list, so a declaration with `P` type parameters and `F` uses of them costs `P + F`, not `P * F`. The reader builds a diagnostic such as `a field name of <declaration>` only when that part fails, so a 65,535-char name costs nothing per field, constant or type parameter.

`SnapshotFormatTest` covers each check and budget, unknown kinds and roles, attributes, truncation at every length, and `User`'s snapshot bytes. A chain of 100,000 declarations, and the same chain closed into a cycle, fail as too deep on a 256 KiB stack, read and written. A generic schema that nests 64 levels through 3^31 instantiations is accepted, and one more level rejected, under a 60-second limit that a walk per instantiation couldn't meet. A record, a sealed type and a value class with 150,000 type parameters and 150,000 uses of the last one are read and rejected under the same limit, which a search of the list per use, about 7 * 10^10 comparisons, doesn't meet. Declarations with 65,535-char names and 100,000 fields, constants and type parameters are read allocating under 64 times the snapshot's size. It also round-trips a schema with every planned shape: a generic record, collections, nullability at several levels, a value class, an enum, nested sealed types and an object.

### Attributes

Each field, each declaration, and the snapshot itself end with an attribute list. Version 1 defines no attribute and writes empty lists. A later release adds information as attributes:

- a **non-critical** attribute is information a reader may ignore. Its absence must mean what it meant before the attribute existed, because a release that doesn't know it drops it when it writes the snapshot again;
- a **critical** attribute changes how the snapshot must be read, so a reader that doesn't know it fails, naming it.

Field attribute tag `1` is reserved for [#35](https://github.com/Lychee-Technology/flinkt/issues/35)'s dropped-value marker, critical. It would say that the old-layout serializer wrote a placeholder for a removed field when Flink carried that state forward ([The old-layout serializer](schema-evolution.md#the-old-layout-serializer)). #35 decides whether it's needed, and its payload. An older reader then fails on the marker instead of reading a placeholder as a value.

## Type identity

- A generated type is its JVM class name, as `Class.getName()` spells it: `com.example.User`, `com.example.Outer$User`. Renaming or moving the class is incompatible. A logical type ID would be a separate decision.
- A field is its Kotlin property name. A rename is a removal and an addition ([#25](schema-evolution.md#values)). Stable field IDs or rename mappings would be a separate, later decision, and only for a concrete use case.
- An enum constant or sealed subtype is its `@FlinkId` within its namespace (#20). The snapshot records each ID's name only to catch an ID swap.
- Persisted order and constructor order are each recorded, the first because the bytes follow it, the second for the hash-code rules ([Restore](#restore)). Neither identifies a field, and neither do `@PersistedFieldOrder`'s keys, which the snapshot doesn't record.

## Restore

`restoreSerializer()` finds the current declaration through the user-code class loader Flink passed to `readSnapshot`, with `TypeRegistry.load`. It builds the current schema for the snapshot's root type and role, and asks the planner ([#34](https://github.com/Lychee-Technology/flinkt/issues/34)) for a plan:

| Plan, or what's found | `restoreSerializer()` |
|---|---|
| as-is | the current codec's serializer, whose snapshot is the old one |
| after migration | the old-layout serializer ([#35](https://github.com/Lychee-Technology/flinkt/issues/35)), whose snapshot is the old schema, exactly. Until a migration's stage lands, the plan reports it as incompatible, naming the operation and the stage |
| incompatible | throws, with the planner's diagnostic: the type, the role, the first difference as a path such as `com.example.User.name`, and the old and current schema of that node |
| the root type's class is gone, or has no codec | throws, naming the type |
| the snapshot's record format version isn't one this release reads | throws, naming the version |

A record's two orders are compared on their own ([Field order](#field-order)):

- **A changed persisted order is incompatible**, in every role: reading the old bytes in the new order would misread them, and no migration stage permutes fields. A separately reviewed and tested stage may add one later. Until then the diagnostic names the old and new order, and says how to keep the layout with `@PersistedFieldOrder`. The fields a migration keeps are compared the same way: they have to stay in the old relative order.
- **A changed constructor order alone is as-is**, except in two places, where what a release persisted depends on a data class's hash code. In the partition-key role it's incompatible, because the key groups would move. Under a `Map` key it's incompatible too, because a codec counts the map's keys by hash code where it's written and where it's read, and a map within the budget could be over it ([Resource limits](#resource-limits)). For a generic declaration, the planner counts a type parameter as under a key in every use of its declaration if it is in one, which can only make it stricter.

It never returns a serializer that could misread the old bytes. A class that's absent only because a removed field used it is fine: the old-layout serializer walks the old schema, not the class, with the same checks a codec makes on each value it reads. Until #35, #12 implements only the as-is branch and throws for everything else. That's the interim implementation of this design, not a different design.

**Incompatible means fail.** Flink calls `restoreSerializer()` at restore for every restored state, whether the job registers it again or not ([What Flink does on restore](schema-evolution.md#what-flink-does-on-restore)). So an incompatible state fails the restore even when the new job no longer uses it. The alternative was a serializer that refuses to read or write but keeps the old snapshot. RocksDB would then carry the state's bytes into every later checkpoint, and the heap backend would still fail, because it reads every entry at restore. The failure would move from the restore that caused it to a later job that registers the state again, and the same savepoint would restore on one backend and fail on the other. A user who drops such a state removes it from the savepoint first, with Flink's State Processor API, or keeps the type compatible. `--allowNonRestoredState` skips the state of operators the new job doesn't have, so it doesn't help while the operator still exists. A carry-forward serializer for RocksDB could be designed later, separately, with backend-specific evidence.

`RestorePrototypeTest` writes state and a snapshot with an old declaration, and restores it through `TypeSerializerSnapshot.readVersionedSnapshot`, with a class loader that holds only the current declaration:

- **`User(id)` restored as `User(id, name: String)`**, which has no migration default. `restoreSerializer()` throws `Can't restore com.example.User (role: value): com.example.User.name was added as kotlin.String, without a migration default (#37). Old: …. Current: …`, with removal enabled too, and `resolveSchemaCompatibility` is incompatible. Read as-is, the same bytes would have given `User(id=1, name=Ada)`.
- **`User(id, name, profile: OldProfile)` restored as `User(id, name)`**, with `OldProfile` and its nested `Address` absent from the class loader. The interim `restoreSerializer()` throws, naming the removal and its stage, #36. With removal enabled, the old-layout serializer reads three records back to back into current `User`s. It consumes each profile, nullable fields and a nullable nested record included, by walking the schema read from the snapshot, and reports the old schema as its snapshot, byte for byte. A corrupt bitmap in a removed value fails the read instead of shifting the next record.
- **A constructor reorder without keys**, `Key(a, b)` to `Key(b, a)`, fails in every role, naming the old and new persisted order.
- **A constructor reorder that keeps the persisted order**, `Key(a, b)` to `Key(@PersistedFieldOrder(7) b, @PersistedFieldOrder(3) a)`, restores as-is as a value and as a map key, and fails as a partition key, naming the old and new constructor order.
- **The same reorder under a `Map` key**: `Holder(keys: Map<Key, Boolean>)` with 1,025 keys `Key(-31 * i, i)`, which the old release wrote. The current codec would fail its read at the 1,025th key, so the restore fails first, with the planner's diagnostic, and `resolveSchemaCompatibility` is incompatible. Renumbering the keys, which changes no order, restores it as-is and reads every key.
- **A removed `Map<String, Long>` that holds a key twice** fails the read in the old-layout serializer, as the old codec fails on the same bytes, though the map's values are never built.
- **A map key** that lost a field fails even with removal enabled.
- **A root type the job no longer has** fails, naming it. So do an unknown snapshot format version and an unknown record format version.

These show the format supports what #35 to #38 will build. They don't ship any migration.

## Evidence

The prototype is test code in `adapters/shared/src/test/kotlin/io/flinkt/flink/format`. The adapters' shared tests run on both PR lanes, so each test runs against Flink 2.3.0 and 1.20.5. The prototype of KSP's `@PersistedFieldOrder` checks is in `flinkt-ksp`'s `persistedOrderPrototype` source set, and its test runs on the primary lane, because KSP's checks don't depend on Flink. Flink 2.2 has no adapter yet ([#27](https://github.com/Lychee-Technology/flinkt/issues/27)), so the 2.3 adapter's `flink22FormatTest` compiles `RecordFormatTest`, `StringFormatTest` and the prototype they test against Flink 2.2.1 and runs them on it, and `FormatFlinkLineTest` fails unless they ran on 2.2.1. The adapter's `check` runs it, so the primary PR lane does. #27's adapter replaces it.

```sh
./gradlew :flinkt-flink23:test --tests 'io.flinkt.flink.format.*'
./gradlew :flinkt-flink120:test --tests 'io.flinkt.flink.format.*'
./gradlew :flinkt-flink23:flink22FormatTest
./gradlew :flinkt-ksp:test
```

| Test | Shows |
|---|---|
| `RecordFormatTest` | the [examples](#examples), with and without `@PersistedFieldOrder`, and every scalar at boundary values, with the same bytes on a `DataOutputSerializer` and a paged view; NaN payloads and signed zero; Flink's built-in serializers' bytes; key bytes equal exactly when keys are equal; a `T?` field's marker; strict reads. Also on 2.2.1 |
| `StringFormatTest` | the string format, byte-equal to `StringValue.writeString` on both views; lossless round trips, unpaired surrogates included; what UTF-8 and `writeUTF` lose; strict reads; bounded allocation. Also on 2.2.1 |
| `ResourceLimitsTest` | the [resource limits](#resource-limits): string and collection budgets on write and read, allocation under corrupt sizes, the zero-width rule, the budget on a map's keys that share a hash code, on write and read, with the comparisons it bounds and the key types `MapKeys` counts or rejects, in linear time for a generic key type of 3^30 instantiations, the limit on a key type's objects, with data objects that share a hash code and the comparisons it bounds, the records a codec counts, the old-layout walker's work and its duplicate-key check |
| `PersistedOrderTest` | constructor order by default, and `@PersistedFieldOrder`'s order; reading into locals in persisted order and calling the constructor once, on Kotlin 2.4.0 and 2.4.21; a reorder without keys reorders the bytes, and one with keys keeps them and moves the key group |
| `PersistedFieldOrderTest`, in `flinkt-ksp` | the persisted order KSP's prototype computes, by default and from sparse and renumbered keys, on Kotlin 2.4.0 and 2.4.21; its errors, each located at its declaration, for partial coverage, a negative key, a repeated key, and an annotation on a body property, a value class's property, a class that isn't a `@FlinkType` and a top-level property |
| `SnapshotFormatTest` | `User`'s snapshot bytes; every planned shape round-trips; ID order; both of a record's orders, recorded and read back, and a malformed constructor order; validation, attributes, budgets, truncation; bounded checks on a 100,000-declaration chain and a generic schema; linear reads and checks of wide declarations and long names; a schema-driven reader that consumes a value of every planned shape but value classes, and rejects a `00` marker in a present `T?` field and a map that holds a key twice |
| `RestorePrototypeTest` | the [restores](#restore), through Flink's versioned snapshot read and a class loader with only the current declaration, the reorders in each role and under a `Map` key included |

What the prototype doesn't show: a real checkpoint or savepoint on either backend, or a restore across adapters. Those are [#12](https://github.com/Lychee-Technology/flinkt/issues/12), [#26](https://github.com/Lychee-Technology/flinkt/issues/26) and [#29](https://github.com/Lychee-Technology/flinkt/issues/29), and testing.md's [snapshot evidence](testing.md#serializer-snapshot-compatibility) applies to them.

## Risks

- **Version 1 is permanent once released.** Until #35's old-layout serializer exists, a change to the record format would make all existing state incompatible. The prototype's tests fix every byte above, and #12 and #15 turn them into golden fixtures of the real codecs, so a change shows up as a failing test, not in a user's restore.
- **An incompatible state that the job no longer uses still fails the restore** ([Restore](#restore)). That's deliberate, and the error names the state's type, but it can surprise someone who removed the state from the code and expects Flink to drop it.
- **Shading.** Relocating `io.flinkt` in a job jar changes the snapshot class name in every checkpoint ([The snapshot class](#the-snapshot-class)).
- **The budgets are judgment.** A schema deeper than 64 levels or larger than 16 MiB fails when its serializer is created, before any state exists. A string over 2^26 chars or a collection over 2^26 elements fails when it's written: on RocksDB the state `update()`, on the heap backend the checkpoint. Raising a budget is compatible, and lowering one never is.
- **No zero-width elements.** An object or a record without fields can't be an element, a field or a type argument ([Resource limits](#resource-limits)). Such a type has one value, so little is lost, but it's a rule users can meet, with an error naming the type.
- **Keys that share a hash code.** The budget limits legitimate maps too. One with more than 1,024 record or value-class keys of one hash code fails when it's written: on RocksDB the state `update()`, on the heap backend the checkpoint, on a network edge the emit. A job that fills such a map from untrusted input already pays for those keys in its own `LinkedHashMap`, which compares each lookup with all of them. A `Map` key type can't hold an enum or an object inside a record or a value class, or more than 1,024 objects, and a record under a `Map` key can't have its constructor reordered, even with its persisted order kept, because that would change the hash codes the budget was checked with ([Resource limits](#resource-limits)).
- **Constructor order is the default layout.** Reordering a record's constructor without `@PersistedFieldOrder` changes its bytes, and the restore fails, though under name order it would have been as-is. The diagnostic says how to keep the layout: give each property a key in the old order. Until #11, the annotation's checks are a prototype.
- **Strings cost a call per byte** through `DataOutput`, as Flink's `StringValue` does. That's unmeasured. Buffering would keep the bytes.

## Not decided here

- What the old-layout serializer writes for a removed field when Flink carries a state forward, and the dropped-value marker's payload: [#35](https://github.com/Lychee-Technology/flinkt/issues/35).
- A migration that permutes fields, which would make a change of persisted order compatible: a separate decision, with its own review and evidence. Until it lands, such a change is incompatible.
- The value-class representation: [#18](https://github.com/Lychee-Technology/flinkt/issues/18). The snapshot already records the wrapper and its underlying property.
- Whether generic classes are supported: [#22](https://github.com/Lychee-Technology/flinkt/issues/22). Their bytes are above if they are.
- Migration defaults for added fields: [#37](https://github.com/Lychee-Technology/flinkt/issues/37).
- Whether the key role takes part in `TypeInformation` equality: [#32](https://github.com/Lychee-Technology/flinkt/issues/32).
