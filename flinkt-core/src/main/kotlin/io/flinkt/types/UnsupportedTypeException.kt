package io.flinkt.types

/**
 * Thrown when Flinkt can't resolve a type to a codec. The message names the canonical type, with its arguments and
 * nullability, and says why.
 */
public class UnsupportedTypeException(
    /** The canonical form of the type that couldn't be resolved, as [FlinktType.toString] gives it. */
    public val canonicalType: String,
    /** Why it couldn't be resolved, without the type's name. */
    public val reason: String,
    cause: Throwable? = null,
) : IllegalArgumentException("Flinkt can't resolve $canonicalType: $reason", cause)
