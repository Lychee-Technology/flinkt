package io.flinkt.schema

import kotlin.reflect.KVariance

/**
 * A type as a schema names it: by strings only, never by a `Class`, so a schema read back from a snapshot can
 * describe a type that the current classpath doesn't have.
 *
 * A field's type is a [SchemaType] with every nested type argument and nullability at every level, so
 * `Map<UserId, List<Event?>>?` keeps everything that tells it apart from another field type. A field of a generic
 * declaration refers to the declaration's type parameters with [TypeParameter].
 */
public sealed class SchemaType {
    /** Whether the type is marked nullable. */
    public abstract val isNullable: Boolean

    /**
     * A class type.
     *
     * [name] is the Kotlin qualified name for a type that Kotlin maps to a Java class, such as `kotlin.Int`,
     * `kotlin.String` or `kotlin.collections.MutableList`, because only that name tells `List` from `MutableList`.
     * Any other class is named by its JVM class name, as `Class.getName()` spells it, such as `com.example.User` or
     * `com.example.Outer$Inner`. Kotlin reserves the `kotlin` package, so the two never collide.
     */
    public class ClassType(
        public val name: String,
        arguments: List<SchemaTypeArgument> = emptyList(),
        override val isNullable: Boolean = false,
    ) : SchemaType() {
        public val arguments: List<SchemaTypeArgument> = arguments.toList()

        init {
            require(name.isNotEmpty()) { "A class type needs a name" }
        }

        override fun equals(other: Any?): Boolean =
            this === other ||
                other is ClassType && name == other.name && isNullable == other.isNullable && arguments == other.arguments

        override fun hashCode(): Int = (name.hashCode() * 31 + arguments.hashCode()) * 31 + isNullable.hashCode()

        override fun toString(): String = buildString {
            append(name)
            if (arguments.isNotEmpty()) arguments.joinTo(this, ", ", "<", ">")
            if (isNullable) append('?')
        }
    }

    /** A reference to a type parameter of the declaration the schema describes, such as `T` in `Envelope<T>`. */
    public class TypeParameter(
        public val name: String,
        override val isNullable: Boolean = false,
    ) : SchemaType() {
        init {
            require(name.isNotEmpty()) { "A type parameter needs a name" }
        }

        override fun equals(other: Any?): Boolean =
            this === other || other is TypeParameter && name == other.name && isNullable == other.isNullable

        override fun hashCode(): Int = name.hashCode() * 31 + isNullable.hashCode()

        override fun toString(): String = if (isNullable) "$name?" else name
    }
}

/** A type argument of a [SchemaType.ClassType]: a type with its use-site variance, or the star projection. */
public class SchemaTypeArgument(
    /** The use-site variance, or `null` for the star projection. */
    public val variance: KVariance?,
    /** The argument's type, or `null` for the star projection. */
    public val type: SchemaType?,
) {
    init {
        require((variance == null) == (type == null)) { "A type argument has a variance and a type, or neither" }
    }

    override fun equals(other: Any?): Boolean =
        this === other || other is SchemaTypeArgument && variance == other.variance && type == other.type

    override fun hashCode(): Int = 31 * variance.hashCode() + type.hashCode()

    override fun toString(): String = when (variance) {
        null -> "*"
        KVariance.INVARIANT -> type.toString()
        KVariance.IN -> "in $type"
        KVariance.OUT -> "out $type"
    }

    public companion object {
        /** The star projection, `*`. */
        public val STAR: SchemaTypeArgument = SchemaTypeArgument(null, null)

        /** An invariant argument, as in `List<User>`. */
        public fun invariant(type: SchemaType): SchemaTypeArgument = SchemaTypeArgument(KVariance.INVARIANT, type)
    }
}
