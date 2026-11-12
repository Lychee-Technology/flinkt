package io.flinkt.types

import kotlin.reflect.KVariance

/**
 * Flinkt's model of one Kotlin type: its classifier, its type arguments, its nullability, and what Flinkt knows
 * about the classifier's shape.
 *
 * A `TypeRegistry` builds the model from a `KType`, as `typeOf<T>()` gives it, and the model keeps everything that
 * `KType` carries without `kotlin-reflect`: the classifier, every nested type argument with its variance, and
 * nullability at every level. It doesn't keep type aliases, imports or the source spelling, because `KType` doesn't
 * have them. Equal `KType`s give equal models.
 *
 * The model represents a type; it doesn't say that Flinkt can serialize it. Resolving a model to a codec is a
 * separate step that fails explicitly for every shape the current release doesn't support.
 *
 * [toString] is the canonical form, which identifies the type in error messages, such as
 * `kotlin.collections.Map<com.example.UserId, kotlin.collections.List<com.example.Event?>>`.
 */
public class FlinktType internal constructor(
    /**
     * The classifier's name. For a class it's the Kotlin qualified name, with the Kotlin name of a type that Kotlin
     * maps to a Java class (`kotlin.Int`, `kotlin.String`, `kotlin.collections.MutableList`) and dots between nested
     * classes (`com.example.Outer.Inner`). A local or anonymous class has no qualified name, so it's its JVM name.
     * For a type parameter, it's the parameter's name, such as `T`.
     */
    public val classifierName: String,
    /**
     * The JVM class of the classifier, or `null` for a type parameter. A primitive is its wrapper class,
     * `java.lang.Integer` for `kotlin.Int`, and `List` and `MutableList` are both `java.util.List`.
     *
     * A generated type is identified by this object, never by its name, so two classes with the same name in two
     * class loaders are two types.
     */
    public val jvmClass: Class<*>?,
    /** The type arguments, in the order of the classifier's type parameters. */
    public val arguments: List<TypeArgument>,
    /** Whether the type is marked nullable, as in `User?`. A platform type from Java isn't. */
    public val isNullable: Boolean,
    /** What Flinkt knows about the classifier. */
    public val shape: TypeShape,
) {
    private val canonical: String = buildString {
        append(classifierName)
        if (arguments.isNotEmpty()) arguments.joinTo(this, ", ", "<", ">")
        if (isNullable) append('?')
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is FlinktType &&
            classifierName == other.classifierName &&
            jvmClass == other.jvmClass &&
            isNullable == other.isNullable &&
            arguments == other.arguments &&
            shape == other.shape

    override fun hashCode(): Int = canonical.hashCode()

    /** The canonical form: the classifier name, the arguments in angle brackets, and `?` when nullable. */
    override fun toString(): String = canonical
}

/**
 * One type argument of a [FlinktType]: a type with its use-site variance, or the star projection.
 *
 * `List<User>` has an invariant argument, `List<out User>` a covariant one, and `List<*>` the star projection.
 */
public class TypeArgument internal constructor(
    /** The use-site variance, or `null` for the star projection. */
    public val variance: KVariance?,
    /** The argument's type, or `null` for the star projection. */
    public val type: FlinktType?,
) {
    init {
        require((variance == null) == (type == null)) { "A type argument has a variance and a type, or neither" }
    }

    override fun equals(other: Any?): Boolean =
        this === other || other is TypeArgument && variance == other.variance && type == other.type

    override fun hashCode(): Int = 31 * variance.hashCode() + type.hashCode()

    override fun toString(): String = when (variance) {
        null -> "*"
        KVariance.INVARIANT -> type.toString()
        KVariance.IN -> "in $type"
        KVariance.OUT -> "out $type"
    }

    public companion object {
        /** The star projection, `*`. */
        public val STAR: TypeArgument = TypeArgument(null, null)
    }
}
