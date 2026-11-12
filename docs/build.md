# Build

This page records how Flinkt is built and why: the module layout, the toolchain, the pinned versions, the checks that keep the architecture's dependency rules, the compile-test harness and the PR CI lanes. The commands are in [AGENTS.md](../AGENTS.md#build-and-test). The evidence each test layer must produce is in [testing.md](testing.md).

## Modules

| Module | Directory | Published | Holds |
|---|---|---|---|
| `flinkt-core` | `flinkt-core/` | yes | `@FlinkType`. Later the type model, codec SPI, persisted schema and planner. Kotlin stdlib only |
| `flinkt-ksp` | `flinkt-ksp/` | yes | the KSP processor (no code yet) |
| `flinkt-view-codegen` | `flinkt-view-codegen/` | no | the forwarder generator for Flinkt's own build (no code yet) |
| `flinkt-flink23` | `adapters/flinkt-flink23/` | yes | the Flink 2.3 adapter, primary |
| `flinkt-flink120` | `adapters/flinkt-flink120/` | yes | the Flink 1.20 adapter |
| `test-support` | `test-support/` | no | the exact-type carrier and the compile-test harness |

Why the modules are separate is in [Layers and modules](architecture.md#layers-and-modules). The production code is only what the checks below need: `@FlinkType` gives the ABI dump and the compile tests a public declaration, and each adapter's line constant gives the shared source a per-line symbol to compile against.

### Adapters

Every adapter compiles `adapters/shared/src/main/kotlin` together with its own `src/main/kotlin`, and runs `adapters/shared/src/test/kotlin` together with its own `src/test/kotlin`. The shared directory is the adapter, and a per-line directory holds only what can't compile on another line ([Differences between target lines](flink-compatibility.md#differences-between-target-lines)). Today that is `ADAPTER_FLINK_LINE`: `FlinkVersion.v2_3` doesn't exist on 1.20. Shared tests run on every adapter lane, so a test that only holds on one line goes in that line's test directory, as the `enableAsyncState()` compile tests do.

Flink is `compileOnly` in an adapter, because the job's environment supplies it ([Dependency packaging](flink-compatibility.md#dependency-packaging)). The same Flink release is on the adapter's test classpath and on its compile-test classpath, so tests run on the line the adapter was compiled for. `AdapterBuildTest` checks that on each lane.

Adding an adapter, such as #27's `flinkt-flink22`, takes a version in `gradle/libs.versions.toml`, two lines in `settings.gradle.kts`, an `adapters/flinkt-flink22/build.gradle.kts` that applies `flinkt.flink-adapter` and names that version, the line's directory with its `ADAPTER_FLINK_LINE`, and an ABI dump from `updateKotlinAbi`. A CI lane for it is a separate decision ([PR CI](#pr-ci)). 2.2 and 2.3 differ in nothing the adapters use yet. If 2.x-only code appears, the per-line directories of 2.2 and 2.3 would duplicate it, and a shared 2.x directory may then be worth adding.

The build logic is in `build-logic/`, as convention plugins: `flinkt.kotlin-jvm` for every module, `flinkt.published` for published modules, `flinkt.flink-adapter` for adapters and `flinkt.compile-tests` for the harness. `build-logic` is an included build, and a task named on the command line doesn't reach one, so the root project's `check` depends on `build-logic`'s. That way `./gradlew build` runs the build logic's own tests too.

## Toolchain

The JDK and the bytecode target come from what the target Flink lines run on. Each line's Java compatibility page, read on November 12, 2026:

| Flink line | Java 8 | Java 11 | Java 17 | Java 21 |
|---|---|---|---|---|
| 1.20 | deprecated, supported | supported, recommended | experimental | not listed |
| 2.2, 2.3 | not supported | supported | default, recommended | experimental |

Flink's own classes match: 1.20.5's are Java 8 class files (major version 52), and 2.2.1's and 2.3.0's are Java 11 (55).

**Bytecode: Java 8 for published modules.** A published class has to run on every JDK that a target line supports, and 1.20 still supports Java 8. `flinkt-core` and generated code are shared by every line, so they need Java 8 anyway. The 2.x adapters could target Java 11, but one target is simpler, and a lower target can't break a user's build: Kotlin refuses to inline a library's bytecode into a compilation whose JVM target is lower than the library's, and Flinkt's view operators are inline. `-Xjdk-release=1.8` holds Kotlin to the Java 8 platform API as well, so a published class can't call a JDK method that Java 8 lacks. A JVM target alone wouldn't catch that. Tests and unpublished modules compile for the toolchain JDK they run on. `test-support` uses Java 9 API.

**JDK: 17, as a Gradle toolchain.** Everything compiles and every test runs on 17. It's Flink 2.x's default, it's supported by every target line (experimentally on 1.20), and Gradle 9 itself needs Java 17 to run. Flink 1.20 on 17 being experimental matters little to PR CI, whose 1.20 lane only compiles and builds stream graphs. Whether merge or nightly CI should run 1.20 on Java 11 as well is for #28. A machine without JDK 17 gets one from the Foojay resolver. CI installs it with `actions/setup-java`.

**Kotlin.** Every module compiles with language and API version 2.4, so the build compiler writes Kotlin metadata 2.4.0 whatever its own version is ([Binary compatibility](kotlin-compatibility.md#four-kinds-of-compatibility)). That includes `test-support`, whose classes the 2.4.0 compiler reads in compile tests. `coreLibrariesVersion` is the minimum compiler's, 2.4.0, so published modules declare `kotlin-stdlib` 2.4.0 and don't raise a user's standard library above what the minimum needs. `flinkt-core`'s and each adapter's tests assert the class-file version and the metadata version of their main classes.

## Pinned versions

`gradle/libs.versions.toml` pins every version the build resolves, with no dynamic selectors:

| Version | Pinned | Why |
|---|---|---|
| `jdk` | 17 | [Toolchain](#toolchain) |
| `jvm-target` | 1.8 | [Toolchain](#toolchain) |
| `kotlin` | 2.4.21 | the build compiler: the newest stable Kotlin, and its key hashes were measured identical to 2.4.0's ([Kotlin compiler compatibility](kotlin-compatibility.md#key-identity-across-compiler-versions)) |
| `kotlin-minimum` | 2.4.0 | the minimum supported compiler |
| `ksp` | 2.3.13 | the newest KSP. It ran with Kotlin 2.4.0 through 2.4.21 |
| `flink23` | 2.3.0 | the only 2.3 release |
| `flink120` | 1.20.5 | the newest 1.20 patch |

Each adapter compiles and tests against one release of its line. Covering the first and the latest patch of each line is merge and nightly work ([CI compatibility matrix](flink-compatibility.md#ci-compatibility-matrix)). Moving the Kotlin version follows [What CI tests](kotlin-compatibility.md#what-ci-tests), which requires the key-hash checks to pass with every release in between.

The Gradle wrapper pins Gradle 9.9.1 with its distribution checksum.

## Public API

Published modules compile in Kotlin's explicit API mode: a declaration without a visibility modifier is an error. Each has an ABI dump at `api/<module>.api`, written by the Kotlin Gradle plugin's ABI validation. `check` runs `checkKotlinAbi`, which fails when the compiled public ABI differs from the dump. After an intended API change, `./gradlew updateKotlinAbi` rewrites the dumps, and their diff is what review reads ([Flinkt API compatibility](testing.md#flinkt-api-compatibility)). Only `flinkt-core`'s dump has content so far.

The plugin's ABI validation DSL is experimental in Kotlin 2.4.21: it needs `@OptIn(ExperimentalAbiValidation::class)`, and a later Kotlin release may change it. If it does, the change is confined to `flinkt.published`. The standalone binary-compatibility-validator plugin writes the same dump format.

## Dependency boundaries

`checkDependencyBoundaries` runs in `check` on every published module. It walks each classpath's resolved dependency graph, so a dependency that arrives transitively fails too. It names the dependency that crosses the boundary and the path that brought it in, but not what that dependency brought in after it.

| Module | Classpath | Rule | Why |
|---|---|---|---|
| every published module | runtime | no `:flinkt-ksp` and no `:flinkt-view-codegen` | build-time modules never reach a runtime classpath ([Layers and modules](architecture.md#layers-and-modules)) |
| every published module | runtime | no `org.apache.flink` module | Flinkt never packages a Flink runtime ([Dependency packaging](flink-compatibility.md#dependency-packaging)) |
| `flinkt-core` | compile and runtime | `kotlin-stdlib` and its `org.jetbrains:annotations` only | `flinkt-core` depends on the Kotlin stdlib only |
| `flinkt-core` | test | no `org.apache.flink` module | the same, for its tests |

The Flink rule for adapters goes by Maven group, so a jar that shades Flink classes under another group would pass it. `flinkt-core`'s rule is an allowlist, so nothing can pass it unnoticed.

**File dependencies.** Gradle leaves a `files(...)` dependency out of the resolved graph, whether the module declares it or gets it from a project it depends on, so the check reads each classpath's file dependencies separately. A file has no group or name for a rule to check, so every rule on its classpath rejects it. A Flink jar added to `flinkt-core` with `implementation(files(...))` fails the allowlist, and with `testImplementation(files(...))` the test rule. The report names the file, but there's no path to name. No checked classpath needs a file dependency. On a published runtime classpath one would be a packaging bug anyway: Gradle doesn't publish file dependencies, so a consumer wouldn't get the file.

The check reads dependencies. A file added straight to a compile task's or a source set's classpath isn't a dependency, and the check doesn't see it.

`DependencyBoundaryCheckTest` in `build-logic` runs the check on Gradle's own dependency resolution, with modules from a Maven repository the test writes, project dependencies and file dependencies.

## Compile-test harness

`:test-support` holds the test code that every module's tests share. It's internal: a published `flinkt-testkit` waits until something outside the repository needs it ([Testkit](testing.md#testkit)). A module gets it by applying `flinkt.compile-tests`, which every published module does.

**Exact static types.** `exactTypeOf(x).shouldBe<T>()` compiles only when the static type of `x` is exactly `T` ([Asserting an exact static type](testing.md#asserting-an-exact-static-type)). Code that must compile uses it directly. Code that must not compile goes through the harness.

**Compiling a snippet.**

```kotlin
KotlinCompilation(KotlinCompiler.MINIMUM)
    .compile(
        """
        import org.apache.flink.streaming.api.datastream.KeyedStream

        fun asyncState(stream: KeyedStream<String, String>) = stream.enableAsyncState()
        """,
    )
    .assertFailure("UNRESOLVED_REFERENCE", "Unresolved reference 'enableAsyncState'")
```

- **Compilers.** `KotlinCompiler.BUILD` is the pinned build compiler and `KotlinCompiler.MINIMUM` is 2.4.0. Each runs inside the test JVM in its own class loader, and the harness checks that the loaded compiler reports the pinned version. A compile takes 0.1 to 0.3 seconds once a compiler is loaded, and about 1.5 seconds the first time in a test JVM (measured locally).
- **Classpath.** By default a snippet compiles against the module as a consumer does: its API and the API of its dependencies, not its `implementation` dependencies. An adapter adds its Flink line, as a user's job has it, and `test-support` adds itself for `exactTypeOf`. So the same shared test runs against 2.3 on one adapter and 1.20 on the other.
- **KSP.** With `kspProcessorClasspath`, the pinned KSP2 runs the processors first, in a child JVM because its command-line entry point ends the JVM it runs in. That adds about 2 seconds per compilation. The compiler then compiles the sources with the Kotlin that KSP generated. Java that KSP generates isn't compiled, and generated resources go into the output classes.
- **Diagnostics.** `assertFailure` requires a compile error, not a crash, and every given text in the errors: a message or a diagnostic name. The two compilers word some messages differently. For a subtype passed to `shouldBe`, 2.4.21 says "Candidate … is inapplicable because of a receiver type mismatch", and 2.4.0 says "Unresolved reference. None of the following candidates is applicable because of a receiver type mismatch". A test that runs on both compilers matches the diagnostic name, here `UNRESOLVED_REFERENCE_WRONG_RECEIVER`, and the wording they share. A KSP error has no name, and it's located at the declaration it's reported on.
- **Running compiled code.** `classLoader()` loads the compiled classes, as the key-hash checks (#32) will need to.

Each compilation keeps its sources, generated code and classes under the module's `build/compile-tests/`, and a failed assertion prints that directory with the full compiler output.

## PR CI

`.github/workflows/pr.yml` runs two lanes on every pull request and on `main`:

- **Primary**, `./gradlew build -x :flinkt-flink120:check`: every module's compilation, tests, ABI and boundary checks, and the build logic's tests, with the 2.3 adapter as the primary adapter ([Pull requests](testing.md#pull-requests)). The 1.20 adapter still compiles here, but its tests run in its own lane.
- **Flink 1.20**, `./gradlew :flinkt-flink120:check -Pflinkt.test.excludeTags=minicluster`: the shared adapter source compiled against 1.20, with the shared and 1.20 tests, ABI and boundary checks, and no MiniCluster.

A test joins a lane by being in a module the lane builds, so later issues add tests without changing the workflow. A test that starts a MiniCluster is tagged `@Tag("minicluster")`, which keeps it off the 1.20 PR lane. The merge, nightly and release tiers are #28 and #30.
