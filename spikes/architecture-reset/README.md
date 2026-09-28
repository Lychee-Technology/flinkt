# Architecture-reset spikes (disposable)

Throwaway experiments behind the architecture review of [PR #1](https://github.com/Lychee-Technology/flinkt/pull/1). They exist to falsify or confirm the Kotlin and Flink assumptions the design depends on. None of this is Flinkt's implementation, and nothing here is built by, or merged into, the main project.

Versions: Kotlin 2.4.20 (`kotlinc`, plus `-language-version 2.0` for the negative probes), Apache Flink 1.20.5, 2.2.1 and 2.3.0, OpenJDK 25. The runtime classpath contains `kotlin-stdlib` but not `kotlin-reflect`.

## What is here

| Path | What it is |
|---|---|
| `inventory/` | Reflection dump of the public/protected API and per-object fields of Flink's stream classes, `TypeInformation`, `TypeSerializer`, snapshots, `RuntimeContext` and state descriptors, per Flink line (`inv-*.tsv`), plus scripts that classify it. Flink 1.20's stability annotations have class retention, so `javap_ann.py` reads them from bytecode. |
| `common/spike/types` | Stand-in type runtime: `typeInfo<T>()` via `typeOf<T>()`, a `ServiceLoader` registry of generated codecs, and adapter-side `TypeInformation`/`TypeSerializer`/`TypeSerializerSnapshot` shared by every generated type. |
| `common/spike/model` | Hand-written stand-ins for KSP output (`User`, `UserEvent`, `Session`). The codec SPI mentions no Flink type. |
| `common/spike/compose` | Composition view: `FlinktStream`/`FlinktOperator`/`FlinktKeyedStream` hold one Flink object each. |
| `common/spike/subtype` | Minimal version of PR #1's subtype façade, for comparison. |
| `common/spike/state` | `*StateDescriptor<T>()` builds descriptors; `RuntimeContext.*State<T>()` binds state. |
| `line120/`, `line2x/` | The only per-line source the spike needed: 1.20 still declares `TypeInformation.createSerializer(ExecutionConfig)` abstract. |
| `probes/` | Resolution and static types, graph and per-object state, MiniCluster run, the `pipeline.generic-types` safety net, exact-type helpers. |
| `docs-api/` | The first-slice view API with the names and signatures the design documents use, and the README's code compiled against it (`build.sh <line> <out> docs-api/*.kt`). Compiles on all three lines. |
| `delegation/` | Kotlin `by` delegation to `DataStream` (doesn't compile: only interfaces can be delegated) and a subclass that forwards by hand (a missed method acts on the subclass's own state; `final union` adds a partition step for a keyed subclass). Run by `run-all.sh`. |
| `view-codegen/` | Gradle project for generated view forwarders. `extractor/` reads each line's parameter names from the LocalVariableTable of Flink's class files with ASM (Flink's jars have no MethodParameters attribute) and writes them, keyed by JVM descriptor, to `views-*/flink-api.tsv`. `processor/` is a KSP processor that generates each view's forwarding members from the Flink classes on the compile classpath, with those names. It carries Flink's `@Deprecated` over as Kotlin `@Deprecated` and Flink's `@Experimental` as the `@ExperimentalFlinkApi` opt-in, and it refuses type-introducing, view-less, `@Internal` and missing methods. `GRADLE=/path/to/gradle view-codegen/run-all.sh` writes `results/view-codegen/`. |
| `neg/` | Compile probes that must fail (N08 is a probe that compiles; see its comment). |
| `results/` | Output of `run-all.sh` for every line. |

## Running

```bash
GRADLE=/path/to/gradle ./setup.sh   # Flink jars into libs/, kotlinc 2.4.20 into tools/
./run-all.sh                        # rewrites results/
```
