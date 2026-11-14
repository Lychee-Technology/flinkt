# Flinkt

**Kotlin-first APIs and compile-time type support for Apache Flink.**

Flinkt is a Kotlin integration layer over Apache Flink. Jobs still use Flink's runtime, programming model, operators and types. Flinkt targets the places where Flink's Java-oriented type extraction loses Kotlin type information, such as nullability and generic arguments.

**Early implementation:** The source tree can build stream graphs using a small set of Flinkt views and Flink's built-in types. Generated serializers, state helpers, and state migration are not implemented. There is no Flinkt release or supported Flink adapter yet. See [project status](#project-status-and-compatibility) before adopting it.

## Why Flinkt?

Flink can accept explicit `TypeInformation` when its type extraction is unsuitable. Flinkt's view can supply it from a Kotlin call site instead. For example, these functions both map a Flink stream of `Long` to `String`:

```kotlin
import io.flinkt.flink.flinkt
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

fun direct(stream: DataStream<Long>): SingleOutputStreamOperator<String> =
    stream.map(MapFunction<Long, String> { it.toString() }, Types.STRING)

fun withFlinkt(stream: DataStream<Long>): SingleOutputStreamOperator<String> =
    stream.flinkt().map { it.toString() }.asFlink()
```

The view's `map` obtains the result type at the call site and passes Flink its `TypeInformation`. For `String`, that is Flink's own built-in type information and serializer. This example builds a transformation; it does not run a job. Both functions are exercised against the two source-tree adapters in [ReadmeExampleTest](adapters/shared/src/test/kotlin/io/flinkt/flink/ReadmeExampleTest.kt).

The longer-term goal is serializers for Kotlin types such as nullable types, `List` and `Map`, data classes, enums and sealed hierarchies, so Flink doesn't have to recover their meaning from JVM classes. None of that type support is available yet.

## Project status and compatibility

| Capability | Status in this source tree |
|---|---|
| Stream views | Partial: `.flinkt()`, `asFlink()`, `map`, `keyBy`, and keyed `process` are implemented, and the views forward a first set of Flink methods, such as `filter`, `name`, `union`, and `sinkTo`. Windows, `connect`, joins, `broadcast`, and `getSideOutput` are not offered. |
| `typeInfo<T>()` | Partial: resolves non-null `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Char`, and `String` to Flink's own type information. Other types fail explicitly. |
| Kotlin type model and codec registry | Implemented in `flinkt-core`. An adapter can't necessarily serialize a type that the model represents. |
| KSP-generated application types and serializers | Planned. `@FlinkType` exists, but annotating a class does not generate a serializer or make `typeInfo<T>()` work for it. |
| State helpers and Flinkt serializer snapshots or migration | Planned; no Flinkt-managed checkpoint or savepoint compatibility is claimed. |

The source tree has adapters compiled against **Flink 2.3.0** and **1.20.5**. Flink 2.2 is a target without an adapter. None has met the [release criteria](docs/testing.md#release-criteria), so none is a supported target yet. The [Flink compatibility policy](docs/flink-compatibility.md#current-support-matrix) describes the intended matrix; its current table predates the two source-tree adapters.

The minimum target Kotlin compiler is **2.4.0**; the build uses 2.4.21. No Kotlin version counts as tested for release yet, because the key-hash checks don't exist. Later compiler versions require the evidence described in the [Kotlin compatibility policy](docs/kotlin-compatibility.md). The build runs on JDK 17 and targets Java 8 bytecode for library modules; applications must also meet their chosen Flink version's Java requirements. See [toolchain details](docs/build.md#toolchain).

## Getting started

### Using Flinkt in an application

There is no [published release](https://github.com/Lychee-Technology/flinkt/releases) yet, and the build has no publication configuration, so there are no coordinates to add to your build. For an eventual integration, the application will need the adapter for its Flink minor line and its own Flink dependencies; Flink is `compileOnly` in the adapters. Application-side KSP configuration for generated types is not available yet.

For now, try the implemented API in the source tree instead of adding Flinkt to an existing job.

### Try the source-tree example

Clone the repository and run the example's graph-building test with the Gradle wrapper. Gradle needs Java 17 or later to run; the build uses a JDK 17 toolchain.

```sh
git clone https://github.com/Lychee-Technology/flinkt.git
cd flinkt
./gradlew :flinkt-flink23:test --tests 'io.flinkt.flink.ReadmeExampleTest'
```

A successful run ends with `BUILD SUCCESSFUL`. The test checks that entering and leaving a view returns the same Flink object and that both mapping functions give Flink its `String` type information. It builds stream graphs; it does not submit a Flink job or prove checkpoint compatibility. To exercise the same test against the 1.20 adapter, replace `flinkt-flink23` in the command with `flinkt-flink120`.

### Building Flinkt from source

Run `./gradlew build` for the repository's compilation, tests, and checks. The wrapper can provision a missing toolchain JDK; Gradle itself still needs a suitable Java runtime. See [build and test instructions](docs/build.md) and the [test strategy](docs/testing.md).

## Flinkt streams

A view holds one existing Flink object; it is not itself a `DataStream`. `.flinkt()` wraps a `DataStream`, `SingleOutputStreamOperator`, or `KeyedStream` without adding a transformation or reading its type. `asFlink()` returns the exact object held by the view. After an operator, that is the object returned by the Flink call, not the original input stream.

The implemented `map` and keyed `process` use a reified result type, and `keyBy` uses a reified key type. They accept Flink's function types and also have overloads taking explicit `TypeInformation`. For generic code or an unsupported result type, supply suitable type information to `map` or `process` explicitly. That picks a Flink serializer, and Flinkt makes no guarantee about it. For keyed `process`, the view calls Flink's public `process` and then `returns` with the output type.

The views also forward a first set of Flink methods that keep the element type. Each calls the Flink method of the same name, with Flink's overloads and parameter names:

- on every view: `filter`, `union`, `rebalance`, `rescale`, `shuffle`, `forward`, `global`, `partitionCustom`, `assignTimestampsAndWatermarks`, `sinkTo`, `print`, `printToErr`, and `executeAndCollect`;
- on an operator or source: `name`, `uid`, `setUidHash`, `setDescription`, `setParallelism`, `setMaxParallelism`, `forceNonParallel`, `setBufferTimeout`, `disableChaining`, `startNewChain`, and `slotSharingGroup`.

A forwarded method whose Flink method returns a stream returns a view of the stream Flink returned, so `.map { … }.name("users").keyBy { … }.process(…)` stays in the view. The sinks and terminal calls return Flink's result as it is: `sinkTo`, `print`, and `printToErr` return Flink's `DataStreamSink`, and `executeAndCollect` returns Flink's `CloseableIterator`, or a `List` when given a limit.

A deprecated Flink method stays deprecated on the view. On the 1.20 adapter that includes overloads 2.x removed, such as `partitionCustom` by field.

A method the view does not offer is a compile error on the view. Call `asFlink()` to use that method on the underlying Flink object, and re-enter with `.flinkt()` if needed. This includes `returns`, `getSideOutput`, `broadcast`, `addSink`, the getters such as `getParallelism`, and on Flink 2.x `enableAsyncState`. Async state enabled on the Flink object before `.flinkt()` is kept. Flink's own type inference and serialization rules apply to calls made outside the view.

## Keys

The view's `keyBy` currently accepts only the nine non-null built-in types listed above. Its explicit-type overload accepts only Flink's corresponding `BasicTypeInfo`, including an equal copy, not a custom type-information subclass. Other keys fail before the view calls Flink; use `asFlink().keyBy(...)` to use Flink's own rules instead. That exit bypasses Flinkt's key checks.

Key serialization and key identity are separate concerns: a change to a key's equality or hash code can make restored state unreachable even if its bytes remain readable. The broader key rules, including those for data-class and value-class keys, exist only as a [design](docs/architecture.md#keys).

<a id="generic-types"></a>
## Type information

`io.flinkt.flink.typeInfo<T>()` reads the Kotlin type, including its arguments and nullability. The core type model can represent more shapes than the adapter can resolve: today `typeInfo<String>()` returns Flink's built-in type information, while `typeInfo<String?>()`, `typeInfo<List<String>>()`, and `typeInfo` for user-defined classes fail with `UnsupportedTypeException`.

<a id="when-something-isnt-supported"></a>
A failure from `typeInfo` does not silently fall back to Flink's generic/Kryo type. Its diagnostic mentions `@FlinkType` as a possible fix, but annotating the type doesn't make `typeInfo` work for it yet. Pass an appropriate `TypeInformation` explicitly where the API accepts one, or use Flink directly. Code outside Flinkt's type resolution can still use Flink's generic serialization; Flinkt does not change the job's Flink configuration.

<a id="keep-flink-recognizable"></a>
<a id="make-unsupported-types-explicit"></a>
## Design principles

- **Keep Flink recognizable.** Operator names, function types, type information, state lifecycle, and Flink configuration remain visible.
- **Preserve Kotlin type information.** The type model retains nested arguments and nullability, even where resolution is not implemented yet.
- **Fail explicitly.** Flinkt's type resolution does not silently choose generic serialization for an unsupported type.
- **Treat state compatibility separately.** A Kotlin API that compiles or a serializer that round-trips does not prove that an old checkpoint or savepoint can be restored.

<a id="generated-serializers"></a>
<a id="sealed-types-and-enums"></a>
<a id="state"></a>
<a id="schema-and-state-compatibility"></a>
<a id="initial-implementation-boundary"></a>
## What comes next

The first end-to-end milestone is a `@FlinkType` data class mapped through a view, keyed and processed with typed state, with serializer snapshots that restore state correctly. Generated serializers are intended to avoid reflection on the per-record path. They will rebuild a data class by calling its primary constructor, so an `init` block may check its arguments but must not change a constructor property. Flinkt can't detect a block that does, and the value would then change on every read and copy ([details](docs/architecture.md#rebuilding-through-the-constructor)). Stable IDs for enum constants and sealed subtypes, and explicit compatibility rules for values and keys, are designed but not implemented. The [schema evolution decision record](docs/schema-evolution.md) separates the target from the stages and evidence still needed to reach it.

## Documentation and contributing

- [Architecture](docs/architecture.md): views, type model, key rules, and design decisions.
- [Schema evolution](docs/schema-evolution.md): planned formats, restore behavior, and migration stages.
- [Flink compatibility](docs/flink-compatibility.md) and [Kotlin compatibility](docs/kotlin-compatibility.md): target versions and support policies.
- [Build](docs/build.md) and [testing](docs/testing.md): modules, development commands, and compatibility evidence.
- [Contributor guidance](AGENTS.md) and [non-code rules](docs/non-code-rules.md): project conventions.
- [Issues](https://github.com/Lychee-Technology/flinkt/issues): report problems and follow planned work.

Flinkt is available under the [MIT License](LICENSE).
