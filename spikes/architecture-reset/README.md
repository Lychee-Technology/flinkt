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
| `neg/` | Compile probes that must fail (N08 is a probe that compiles; see its comment). |
| `results/` | Output of `run-all.sh` for every line. |

## Running

```bash
GRADLE=/path/to/gradle ./setup.sh   # Flink jars into libs/, kotlinc 2.4.20 into tools/
./run-all.sh                        # rewrites results/
```
