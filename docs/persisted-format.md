# Persisted format

This document records the bytes Flinkt persists, record format version 1 and snapshot format version 1, and what `restoreSerializer()` returns. It is the decision record for [#5](https://github.com/Lychee-Technology/flinkt/issues/5). Its direction was approved on the issue ([approved decisions](https://github.com/Lychee-Technology/flinkt/issues/5#issuecomment-6872410065)). This document fixes the exact bytes. [schema-evolution.md](schema-evolution.md) records how a restore decides what to do with them, and [architecture.md](architecture.md#persisted-formats) records the collection, ID and key decisions built on them.

Nothing here is implemented yet. KSP generates the codecs in [#11](https://github.com/Lychee-Technology/flinkt/issues/11), the adapters' serializer and snapshot class come in [#12](https://github.com/Lychee-Technology/flinkt/issues/12), and nullable types in [#15](https://github.com/Lychee-Technology/flinkt/issues/15). The evidence is a prototype in the adapters' shared tests, [`adapters/shared/src/test/kotlin/io/flinkt/flink/format`](../adapters/shared/src/test/kotlin/io/flinkt/flink/format), which both PR lanes run, on Flink 2.3.0 and 1.20.5 ([Evidence](#evidence)).

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

Record format version 1. Generated codecs write through `java.io.DataOutput` and read through `DataInput`, and mention no Flink type ([Generated code and the adapter](architecture.md#generated-code-and-the-adapter)). They use only `writeByte`, `writeShort`, `writeChar`, `writeInt` and `writeLong`, which write the same bytes on Flink's output views. `RecordFormatTest` checks it on a `DataOutputSerializer` and on a paged view (`AbstractPagedOutputView`), the two views whose floating-point writes differ, on 1.20.5 and 2.3.0. Their bytecode on 2.2.1 is the same as on 2.3.0. `writeFloat`, `writeDouble`, `writeBytes(String)` and `writeUTF` are never used.

### Field order

A record's fields are written in ascending `String.compareTo` order of their Kotlin property names, the names the schema records (`FieldSchema.name`). That order compares UTF-16 code units, so it's the same in every JVM and every locale. It's also the order `PojoTypeInfo` sorts a POJO's fields in (`Field.getName().compareTo`, checked in the 2.3.0 bytecode).

A field is identified by its name, so a pure source reorder writes the same bytes. `Account(id, name, email, balance, rating)` is written as `balance`, `email`, `id`, `name`, `rating`.

A generated codec reads each field into a local in that order, then calls the primary constructor once, in declaration order. Construction is ordinary construction: an `init` block runs once and sees every argument, so a check across fields works. `CanonicalOrderTest` decodes `Key(a, b)` and the reordered `Key(b, a)` from the same bytes, compiled by Kotlin 2.4.0 and 2.4.21, and an `init` block that compares `a` with `b` passes or fails as it would for a direct call.

Alternatives:

- **Constructor order** would change the bytes on a reorder, which [#25](schema-evolution.md#values) rules out.
- **A `Collator`** depends on the locale.
- **Code-point order** differs from `compareTo` only for names that mix supplementary characters with characters from U+E000 to U+FFFF, and gains nothing.

Constructor order still matters to a partition key, because a data class's generated `hashCode()` follows it. `CanonicalOrderTest` shows `Key(a = 1, b = 2)` and its reordered declaration writing `00 00 00 01 00 00 00 02` both times, and Flink's `KeyGroupRangeAssignment` filing them under key groups 29 and 44 at max parallelism 128. So the snapshot records constructor order ([Content](#content)).

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

Generated code doesn't call Flink to write it. `StringFormatTest` compares Flinkt's bytes with `StringValue.writeString`'s over fixed cases, every length where the length's varint grows, a 70,000-char string and 500 random strings of arbitrary UTF-16 code units, on both views. Each reads what the other wrote. `writeString` and `readString` have the same bytecode in 1.20.5, 2.2.1 and 2.3.0.

The format is lossless for every `String`, so two different strings never have the same bytes, which key determinism needs ([Deterministic bytes](#deterministic-bytes)). It has no length limit below `Int.MAX_VALUE - 1` chars. Alternatives:

- **UTF-8 through `String.toByteArray`** writes an unpaired surrogate as `?`, the same byte as `"?"`. `StringFormatTest` checks it.
- **`writeUTF`** fails on an encoding over 65,535 bytes.
- **Two bytes per char** would double every ASCII string and gain nothing.
- **A lossless UTF-8 variant** (CESU-8 or WTF-8) would need an encoder of its own. A field would then have other bytes than the same string at the top level, where Flink's `StringSerializer` writes it.

The choice doesn't rest on speed, and nothing was measured. If #12 finds the per-char writes slow, an encoder can buffer them and still write the same bytes. A different encoding would be a new record format version.

### Null representation

**The null bitmap.** A record with nullable fields starts with a bitmap, before any field:

- one bit per nullable field, in canonical field order restricted to the nullable fields;
- `ceil(n / 8)` bytes for `n` nullable fields;
- bit `k` is in byte `k / 8`, as the value `1 shl (k % 8)`: least significant bit first;
- `1` means null and `0` present. A null field has no bytes after the bitmap;
- unused bits in the last byte are `0`. A set unused bit is malformed;
- no bitmap at all when the record has no nullable field.

Which fields are nullable is decided by the declaration: a field whose declared type is marked nullable, such as `String?`, `User?` or `T?`. A field declared as a bare type parameter `T` isn't in the bitmap, even when its argument is nullable. Its value is written by the argument's codec, which for a nullable argument writes a marker. A present `T?` field is written by its argument's codec too, so with a nullable argument it has a bitmap bit and then a `01` marker. So a record's layout, bitmap included, depends only on its declaration, never on its type arguments. Whether generic classes are supported is [#22](https://github.com/Lychee-Technology/flinkt/issues/22)'s decision, and if they are, these are their bytes.

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

- a `Boolean` or null marker other than `00` or `01`;
- a set unused bitmap bit;
- a `Float` or `Double` NaN other than the canonical one;
- a string length of `00`, a varint with a redundant last `00` byte, a length varint over 5 bytes or `Int.MAX_VALUE`, a char varint over 3 bytes or U+FFFF;
- an enum or sealed ID the type doesn't have ([#20](architecture.md#enum-and-sealed-identity)), a negative collection size ([#16](architecture.md#collections));
- truncated input, as `EOFException`.

Each is one comparison on a path the codec takes anyway. Flink's own readers accept some of these, such as a non-canonical NaN or a non-minimal varint. Nothing Flinkt writes produces them.

A record's nesting is bounded by its schema, which has no cycles, so a codec needs no depth limit. What a corrupt length could make a reader allocate is bounded instead. A string reader starts with at most 8,192 chars and doubles as it reads, so it never holds more than that first chunk or twice what it has read, and a corrupt length fails at the end of the input. `StringFormatTest` reads a length of 2,147,483,646 chars followed by three chars, and measures under 1 MB allocated before the `EOFException`. Flink's `StringValue.readString` allocates the claimed length, when it's over 2,048, before it reads a char. Collection readers (#17) follow the same rule for sizes.

### Where the codecs live

Generated code writes a built-in field itself, in these formats. A Flinkt codec for a built-in is needed only where one is composed, as a `List<String>` element (#17) or an `Envelope<String>` argument (#22). Those codecs, and the format primitives generated code calls, live in `flinkt-core`, which has no Flink dependency. They aren't registered through the codec SPI, which still has no way for a module to register a built-in's codec, so no jar on the classpath can redefine the bytes of `Int` or `String`. #17 or #22, whichever comes first, adds them. A top-level built-in stays Flink's own `TypeInformation` and serializer (#7). The prototype's primitives are `RecordFormat`, in test code.

### Examples

Each example is written through a `DataOutputSerializer` and through a paged view, with the same bytes on both. `RecordFormatTest` asserts the bytes and the round trip, and #12 and #15 turn them into golden fixtures.

```text
User(id: Long, name: String)

User(42, "Ada")
00 00 00 00 00 00 00 2a     id
04 41 64 61                 name
```

The nullable variant has a constructor order that isn't its canonical order:

```text
Account(id: Long, name: String?, email: String?, balance: Double?, rating: Float)
canonical order: balance, email, id, name, rating. Bitmap: balance bit 0, email bit 1, name bit 2.

Account(7, "Ada", null, Double.fromBits(0x7ff0000000000001), Float.fromBits(0x7f800001))
02                          bitmap: email is null
7f f8 00 00 00 00 00 00     balance: a NaN with a payload, written as the canonical NaN
00 00 00 00 00 00 00 07     id
04 41 64 61                 name
7f c0 00 00                 rating: the same for Float

Account(7, null, null, null, -0.0f)
07                          bitmap: balance, email and name are null
00 00 00 00 00 00 00 07     id
80 00 00 00                 rating: negative zero
```

```text
Session(user: User, note: String?, previous: User?)
canonical order: note, previous, user. Bitmap: note bit 0, previous bit 1.

Session(User(42, "Ada"), "hi", User(41, "Al"))
00 | 03 68 69 | 00 00 00 00 00 00 00 29 03 41 6c | 00 00 00 00 00 00 00 2a 04 41 64 61

Session(User(42, "Ada"), null, null)
03 | 00 00 00 00 00 00 00 2a 04 41 64 61

Wide(a … i: Boolean?), nine nullable fields: two bitmap bytes
all null              ff 01
only a = true         fe 01 | 01
only i = false        ff 00 | 00

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
  - a record: its type parameters, and its fields in **primary-constructor order**, each with its name and full type;
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
| field identity by name, canonical order | field names. The record format version defines the order |
| constructor order | the order of a record's fields |
| the key role | the role byte |

Enum constants and sealed subtypes are written in ID order, so declaring them in another order changes neither the records nor the snapshot. A record's fields are in constructor order, which is how the snapshot records that order, and the canonical order the bytes use is derived from their names. So declaring a record's fields in another order changes the snapshot and not the layout. `SnapshotFormatTest` checks both.

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
    1 record       Int type-parameter count, String each; Int field count, Field each, in constructor order
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

`User(id: Long, name: String)` in the value role is 120 bytes of data:

```text
00 00 00 01                     record format version 1
00                              role: value
01 11 "com.example.User" 00 00 00 00 00
                                root: class type com.example.User, not nullable, no arguments
00 00 00 01                     one declaration
01 11 "com.example.User"        a record, com.example.User
00 00 00 00                     no type parameters
00 00 00 02                     two fields, in constructor order
03 "id"    01 0c "kotlin.Long"   00 00 00 00 00    00 00 00 00
05 "name"  01 0e "kotlin.String" 00 00 00 00 00    00 00 00 00
                                each: name, type, no attributes
00 00 00 00                     no declaration attributes
00 00 00 00                     no snapshot attributes
```

`SnapshotFormatTest` has these bytes in full. A user's checkpoint holds them after Flink's framing: the class name with `writeUTF`, then the snapshot format version as an `Int`, then the data, then the adapter's part, here `00 00 00 00`.

Fixed-width integers make a snapshot larger than varints would: with one-byte varints, `User`'s would be 87 bytes instead of 120. A snapshot is written once per state per checkpoint, not per record. Fixed widths keep it readable as hex and match the record format.

### Nested Flink snapshots

A Flinkt serializer delegates to a Flink serializer in one place in version 1: a top-level nullable built-in, such as `typeInfo<String?>()`, wraps Flink's `StringSerializer`. Its bytes after the marker are Flink's, so Flink's snapshot of that serializer decides their compatibility. The adapter's part of the snapshot nests it, written and read with Flink's own versioned snapshot methods. There's exactly one when the root type is a nullable built-in, and none otherwise. A restore is as-is only when the schemas are identical and Flink resolves the nested snapshot as-is. #15 implements it, and the prototype writes the empty case.

### Validation and budgets

The writer and the reader run the same checks, so a release never writes a snapshot it would reject, and never rejects one it could have written:

- each declaration appears once, in ascending name order;
- every class type is a built-in, a collection or a declared type, with the number of type arguments it takes, and no star projection;
- a type parameter is referenced only inside a declaration that declares it, and the root type references none;
- every declaration is reachable from the root, and none reaches itself. The first release supports no recursive type;
- a record's field names are unique, and enum and sealed IDs are positive and ascending. A sealed subtype is a declared record, object or sealed type;
- names aren't empty;
- an unknown kind, variance, role or critical attribute fails, naming it.

Corrupt input is also kept from exhausting memory or stack:

| Budget | Version 1 | Why this number |
|---|---|---|
| snapshot data | 16 MiB | far beyond any schema we expect, and small enough to hold in memory |
| a name | 65,535 chars | a JVM class file can't hold a longer class, field or type-parameter name |
| nesting | 64 levels | how deep reading a value recurses, in a codec or in the old-layout serializer walking a removed value: one level per record, collection, value class or sealed type, through type arguments as substituted |

No list is allocated from a count. Each element is read before it's added, and each takes at least a byte, so the size budget bounds every list. The writer enforces the same budgets when a serializer is created, before anything is persisted. A legitimate checkpoint therefore never exceeds them. The numbers are judgment, not measurement. A later release may raise a budget, but never lower one.

`SnapshotFormatTest` covers each check and budget, unknown kinds and roles, attributes, truncation at every length, and `User`'s snapshot bytes. It also round-trips a schema with every planned shape: a generic record, collections, nullability at several levels, a value class, an enum, nested sealed types and an object.

### Attributes

Each field, each declaration, and the snapshot itself end with an attribute list. Version 1 defines no attribute and writes empty lists. A later release adds information as attributes:

- a **non-critical** attribute is information a reader may ignore. Its absence must mean what it meant before the attribute existed, because a release that doesn't know it drops it when it writes the snapshot again;
- a **critical** attribute changes how the snapshot must be read, so a reader that doesn't know it fails, naming it.

Field attribute tag `1` is reserved for [#35](https://github.com/Lychee-Technology/flinkt/issues/35)'s dropped-value marker, critical. It would say that the old-layout serializer wrote a placeholder for a removed field when Flink carried that state forward ([The old-layout serializer](schema-evolution.md#the-old-layout-serializer)). #35 decides whether it's needed, and its payload. An older reader then fails on the marker instead of reading a placeholder as a value.

## Type identity

- A generated type is its JVM class name, as `Class.getName()` spells it: `com.example.User`, `com.example.Outer$User`. Renaming or moving the class is incompatible. A logical type ID would be a separate decision.
- A field is its Kotlin property name. A rename is a removal and an addition ([#25](schema-evolution.md#values)). Stable field IDs or rename mappings would be a separate, later decision, and only for a concrete use case.
- An enum constant or sealed subtype is its `@FlinkId` within its namespace (#20). The snapshot records each ID's name only to catch an ID swap.
- Constructor order is recorded beside the fields, for the partition-key rule, and identifies nothing.

## Restore

`restoreSerializer()` finds the current declaration through the user-code class loader Flink passed to `readSnapshot`, with `TypeRegistry.load`. It builds the current schema for the snapshot's root type and role, and asks the planner ([#34](https://github.com/Lychee-Technology/flinkt/issues/34)) for a plan:

| Plan, or what's found | `restoreSerializer()` |
|---|---|
| as-is | the current codec's serializer, whose snapshot is the old one |
| after migration | the old-layout serializer ([#35](https://github.com/Lychee-Technology/flinkt/issues/35)), whose snapshot is the old schema, exactly. Until a migration's stage lands, the plan reports it as incompatible, naming the operation and the stage |
| incompatible | throws, with the planner's diagnostic: the type, the role, the first difference as a path such as `com.example.User.name`, and the old and current schema of that node |
| the root type's class is gone, or has no codec | throws, naming the type |
| the snapshot's record format version isn't one this release reads | throws, naming the version |

It never returns a serializer that could misread the old bytes. A class that's absent only because a removed field used it is fine: the old-layout serializer walks the old schema, not the class, with the same checks a codec makes on each value it reads. Until #35, #12 implements only the as-is branch and throws for everything else. That's the interim implementation of this design, not a different design.

**Incompatible means fail.** Flink calls `restoreSerializer()` at restore for every restored state, whether the job registers it again or not ([What Flink does on restore](schema-evolution.md#what-flink-does-on-restore)). So an incompatible state fails the restore even when the new job no longer uses it. The alternative was a serializer that refuses to read or write but keeps the old snapshot. RocksDB would then carry the state's bytes into every later checkpoint, and the heap backend would still fail, because it reads every entry at restore. The failure would move from the restore that caused it to a later job that registers the state again, and the same savepoint would restore on one backend and fail on the other. A user who drops such a state removes it from the savepoint first, with Flink's State Processor API, or keeps the type compatible. `--allowNonRestoredState` skips the state of operators the new job doesn't have, so it doesn't help while the operator still exists. A carry-forward serializer for RocksDB could be designed later, separately, with backend-specific evidence.

`RestorePrototypeTest` writes state and a snapshot with an old declaration, and restores it through `TypeSerializerSnapshot.readVersionedSnapshot`, with a class loader that holds only the current declaration:

- **`User(id)` restored as `User(id, name: String)`**, which has no migration default. `restoreSerializer()` throws `Can't restore com.example.User (role: value): com.example.User.name was added as kotlin.String, without a migration default (#37). Old: …. Current: …`, with removal enabled too, and `resolveSchemaCompatibility` is incompatible. Read as-is, the same bytes would have given `User(id=1, name=Ada)`.
- **`User(id, name, profile: OldProfile)` restored as `User(id, name)`**, with `OldProfile` and its nested `Address` absent from the class loader. The interim `restoreSerializer()` throws, naming the removal and its stage, #36. With removal enabled, the old-layout serializer reads three records back to back into current `User`s. It consumes each profile, nullable fields and a nullable nested record included, by walking the schema read from the snapshot, and reports the old schema as its snapshot, byte for byte. A corrupt bitmap in a removed value fails the read instead of shifting the next record.
- **A constructor reorder** of `Key(a, b)` restores as-is as a value, and fails as a partition key, naming the old and new order.
- **A map key** that lost a field fails even with removal enabled.
- **A root type the job no longer has** fails, naming it. So do an unknown snapshot format version and an unknown record format version.

These show the format supports what #35 to #38 will build. They don't ship any migration.

## Evidence

The prototype is test code in `adapters/shared/src/test/kotlin/io/flinkt/flink/format`. The adapters' shared tests run on both PR lanes, so each test runs against Flink 2.3.0 and 1.20.5:

```sh
./gradlew :flinkt-flink23:test --tests 'io.flinkt.flink.format.*'
./gradlew :flinkt-flink120:test --tests 'io.flinkt.flink.format.*'
```

| Test | Shows |
|---|---|
| `RecordFormatTest` | the [examples](#examples) and every scalar at boundary values, with the same bytes on a `DataOutputSerializer` and a paged view; NaN payloads and signed zero; Flink's built-in serializers' bytes; key bytes equal exactly when keys are equal; strict reads |
| `StringFormatTest` | the string format, byte-equal to `StringValue.writeString` on both views; lossless round trips, unpaired surrogates included; what UTF-8 and `writeUTF` lose; strict reads; bounded allocation |
| `CanonicalOrderTest` | reading into locals in canonical order and calling the constructor once, on Kotlin 2.4.0 and 2.4.21; a reorder keeps the bytes and moves the key group |
| `SnapshotFormatTest` | `User`'s snapshot bytes; every planned shape round-trips; ID order; validation, attributes, budgets, truncation; a schema-driven reader that consumes a value of every planned shape but value classes |
| `RestorePrototypeTest` | the [restores](#restore), through Flink's versioned snapshot read and a class loader with only the current declaration |

Flink 2.2 has no adapter yet ([#27](https://github.com/Lychee-Technology/flinkt/issues/27)). For 2.2.1, `javap -c` shows that `StringValue.writeString` and `readString`, and the `write*` methods of `DataOutputSerializer` and `AbstractPagedOutputView`, have the same bytecode as in 2.3.0. In 1.20.5, the views' `write*` methods are the same too, except that `DataOutputSerializer` calls its `resize` with `invokespecial` where 2.2.1 and 2.3.0 use `invokevirtual`. 2.2 joins the tests when its adapter does.

What the prototype doesn't show: a real checkpoint or savepoint on either backend, or a restore across adapters. Those are [#12](https://github.com/Lychee-Technology/flinkt/issues/12), [#26](https://github.com/Lychee-Technology/flinkt/issues/26) and [#29](https://github.com/Lychee-Technology/flinkt/issues/29), and testing.md's [snapshot evidence](testing.md#serializer-snapshot-compatibility) applies to them.

## Risks

- **Version 1 is permanent once released.** Until #35's old-layout serializer exists, a change to the record format would make all existing state incompatible. The prototype's tests fix every byte above, and #12 and #15 turn them into golden fixtures of the real codecs, so a change shows up as a failing test, not in a user's restore.
- **An incompatible state that the job no longer uses still fails the restore** ([Restore](#restore)). That's deliberate, and the error names the state's type, but it can surprise someone who removed the state from the code and expects Flink to drop it.
- **Shading.** Relocating `io.flinkt` in a job jar changes the snapshot class name in every checkpoint ([The snapshot class](#the-snapshot-class)).
- **The budgets are judgment.** A schema deeper than 64 levels or larger than 16 MiB fails when its serializer is created, before any state exists. Raising a budget is compatible, and lowering one never is.
- **Strings cost a call per byte** through `DataOutput`, as Flink's `StringValue` does. That's unmeasured. Buffering would keep the bytes.

## Not decided here

- What the old-layout serializer writes for a removed field when Flink carries a state forward, and the dropped-value marker's payload: [#35](https://github.com/Lychee-Technology/flinkt/issues/35).
- The value-class representation: [#18](https://github.com/Lychee-Technology/flinkt/issues/18). The snapshot already records the wrapper and its underlying property.
- Whether generic classes are supported: [#22](https://github.com/Lychee-Technology/flinkt/issues/22). Their bytes are above if they are.
- Migration defaults for added fields: [#37](https://github.com/Lychee-Technology/flinkt/issues/37).
- Whether the key role takes part in `TypeInformation` equality: [#32](https://github.com/Lychee-Technology/flinkt/issues/32).
