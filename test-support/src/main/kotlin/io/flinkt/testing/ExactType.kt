package io.flinkt.testing

/**
 * The invariant carrier that compile contracts assert an exact static type with (docs/testing.md, "Asserting an
 * exact static type"):
 *
 * ```kotlin
 * exactTypeOf(users).shouldBe<FlinktSingleOutputStreamOperator<User>>()
 * ```
 *
 * [exactTypeOf] infers `Exactly<X>` from its argument alone, before any expected type applies, and `Exactly` is
 * invariant, so [shouldBe] compiles only when `X` is exactly the type argument. A subtype doesn't match. A flexible
 * type such as `User!` matches `User`. The assertion happens at compile time: code that uses it either compiles or
 * doesn't, and a compile test checks that the subtype case doesn't.
 */
public class Exactly<T> internal constructor()

/** Captures the static type of [value]. */
public fun <T> exactTypeOf(@Suppress("UNUSED_PARAMETER") value: T): Exactly<T> = Exactly()

/** Compiles only if the receiver's type argument is exactly [U]. */
public fun <U> Exactly<U>.shouldBe() {}
