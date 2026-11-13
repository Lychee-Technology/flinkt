package io.flinkt.types

import io.flinkt.schema.DeclarationSchema

/**
 * What Flinkt knows about a type's classifier.
 *
 * The shape of a built-in, a collection or a type parameter follows from the classifier. A generated declaration's
 * shape (record, value class, enum, sealed hierarchy or object) comes from the schema its module registered, because
 * `typeOf<T>()` can't tell a sealed class or a value class apart without `kotlin-reflect`. Any other class is known
 * by its classifier only.
 *
 * A shape says what the type is, not whether Flinkt can serialize it yet.
 */
public sealed class TypeShape {
    /** One of the types that Flink has built-in serializers for and Flinkt treats as scalars. */
    public class BuiltIn internal constructor(public val kind: BuiltInKind) : TypeShape() {
        override fun equals(other: Any?): Boolean = other is BuiltIn && kind == other.kind

        override fun hashCode(): Int = kind.hashCode()

        override fun toString(): String = "BuiltIn($kind)"
    }

    /** A collection or array type. Its element, key and value types are the model's type arguments. */
    public class Collection internal constructor(public val kind: CollectionKind) : TypeShape() {
        override fun equals(other: Any?): Boolean = other is Collection && kind == other.kind

        override fun hashCode(): Int = kind.hashCode()

        override fun toString(): String = "Collection($kind)"
    }

    /** A generated declaration, whose structure is the [schema] its `GeneratedTypeModule` registered. */
    public class Generated internal constructor(public val schema: DeclarationSchema) : TypeShape() {
        override fun equals(other: Any?): Boolean = other is Generated && schema == other.schema

        override fun hashCode(): Int = schema.hashCode()

        override fun toString(): String = "Generated(${schema.typeName})"
    }

    /** A class Flinkt has no metadata for: no registered schema, and not a built-in or a collection. */
    public object ClassifierOnly : TypeShape() {
        override fun toString(): String = "ClassifierOnly"
    }

    /** A type parameter, as in `typeOf<List<T>>()` inside a class or function where `T` isn't reified. */
    public object TypeParameter : TypeShape() {
        override fun toString(): String = "TypeParameter"
    }
}

/** The built-in scalar types. Each is non-null; nullability is the model's. */
public enum class BuiltInKind(
    /** The Kotlin qualified name, such as `kotlin.Int`. */
    public val kotlinName: String,
    /** The JVM class that represents a value of the type: the wrapper class for a primitive. */
    public val jvmClass: Class<*>,
) {
    BOOLEAN("kotlin.Boolean", Boolean::class.javaObjectType),
    BYTE("kotlin.Byte", Byte::class.javaObjectType),
    SHORT("kotlin.Short", Short::class.javaObjectType),
    INT("kotlin.Int", Int::class.javaObjectType),
    LONG("kotlin.Long", Long::class.javaObjectType),
    FLOAT("kotlin.Float", Float::class.javaObjectType),
    DOUBLE("kotlin.Double", Double::class.javaObjectType),
    CHAR("kotlin.Char", Char::class.javaObjectType),
    STRING("kotlin.String", String::class.java),
}

/**
 * The collection and array types the model tells apart.
 *
 * Kotlin's read-only and mutable collection interfaces are the same JVM interface, so only the `KType` tells
 * `List` from `MutableList`. A platform type from Java, `(Mutable)List<T>!`, is modeled as the read-only type.
 */
public enum class CollectionKind(
    /** The Kotlin qualified name of the classifier, such as `kotlin.collections.List`, or `null` for any primitive array. */
    public val kotlinName: String?,
) {
    LIST("kotlin.collections.List"),
    MAP("kotlin.collections.Map"),
    SET("kotlin.collections.Set"),
    COLLECTION("kotlin.collections.Collection"),
    ITERABLE("kotlin.collections.Iterable"),
    MUTABLE_LIST("kotlin.collections.MutableList"),
    MUTABLE_MAP("kotlin.collections.MutableMap"),
    MUTABLE_SET("kotlin.collections.MutableSet"),
    MUTABLE_COLLECTION("kotlin.collections.MutableCollection"),
    MUTABLE_ITERABLE("kotlin.collections.MutableIterable"),

    /** `Array<T>`. */
    ARRAY("kotlin.Array"),

    /** `IntArray` and the other primitive arrays, whose classifier name says which. */
    PRIMITIVE_ARRAY(null),
}
