package io.flinkt.registry

import io.flinkt.types.BuiltInKind
import io.flinkt.types.CollectionKind
import io.flinkt.types.FlinktType
import io.flinkt.types.TypeArgument
import io.flinkt.types.TypeShape
import io.flinkt.types.UnsupportedTypeException
import kotlin.jvm.internal.Reflection
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.KTypeParameter
import kotlin.reflect.KTypeProjection

/**
 * Structural analysis: builds the [FlinktType] of a [KType], with the shape of each generated declaration taken from
 * [declarationOf].
 *
 * It reads only what `KType` offers without `kotlin-reflect`. Where `KType` hides something behind equality alone,
 * which is how `typeOf` marks a mutable collection or `Nothing`, the analyzer rebuilds the read-only form through
 * `kotlin.jvm.internal.Reflection`, the factory that compiled `typeOf` calls use, and compares. With `kotlin-reflect`
 * on the classpath that factory builds `kotlin-reflect`'s types instead, so the comparison holds for both.
 */
internal class TypeAnalyzer(private val declarationOf: (Class<*>) -> TypeShape.Generated?) {
    fun analyze(type: KType): FlinktType {
        val arguments = type.arguments.map(::argument)
        return when (val classifier = type.classifier) {
            is KClass<*> -> classType(type, classifier, arguments)
            is KTypeParameter -> FlinktType(classifier.name, null, arguments, type.isMarkedNullable, TypeShape.TypeParameter)
            else -> throw UnsupportedTypeException(type.toString(), "it has no classifier, so Flinkt can't model it")
        }
    }

    /**
     * [type], which another registry may have built, with the shape of each class at every depth taken from
     * [declarationOf], as [analyze] would give it here. Only a generated declaration's shape depends on the registry:
     * a built-in, a collection or a type parameter is the same everywhere, so it's kept. Returns [type] itself when
     * nothing changes.
     */
    fun reconcile(type: FlinktType): FlinktType {
        val arguments = type.arguments.map { argument ->
            val argumentType = argument.type ?: return@map argument
            val reconciled = reconcile(argumentType)
            if (reconciled === argumentType) argument else TypeArgument(argument.variance, reconciled)
        }
        val shape = when (type.shape) {
            is TypeShape.Generated, TypeShape.ClassifierOnly -> declarationOf(checkNotNull(type.jvmClass)) ?: TypeShape.ClassifierOnly
            else -> type.shape
        }
        val unchanged = shape == type.shape && arguments.indices.all { arguments[it] === type.arguments[it] }
        return if (unchanged) type else FlinktType(type.classifierName, type.jvmClass, arguments, type.isNullable, shape)
    }

    private fun argument(projection: KTypeProjection): TypeArgument {
        val variance = projection.variance ?: return TypeArgument.STAR
        return TypeArgument(variance, analyze(checkNotNull(projection.type)))
    }

    private fun classType(type: KType, classifier: KClass<*>, arguments: List<TypeArgument>): FlinktType {
        // A primitive's classifier is the primitive class; the model uses its wrapper, as every other generic use does.
        val jvmClass = classifier.javaObjectType
        val mutableName = MUTABLE_NAMES[jvmClass]?.takeIf { isMutableCollection(type, jvmClass) }
        val name = when {
            mutableName != null -> mutableName
            jvmClass == Void::class.java && isNothing(type) -> "kotlin.Nothing"
            else -> classifier.qualifiedName ?: jvmClass.name
        }
        val shape = BUILT_INS[jvmClass]?.let(TypeShape::BuiltIn)
            ?: collectionKind(jvmClass, mutable = mutableName != null)?.let(TypeShape::Collection)
            ?: declarationOf(jvmClass)
            ?: TypeShape.ClassifierOnly
        // A platform type from Java isn't marked nullable, so it's modeled as non-null.
        return FlinktType(name, jvmClass, arguments, type.isMarkedNullable, shape)
    }

    /**
     * Whether [type] is Kotlin's mutable view of [jvmClass], such as `MutableList<T>` rather than `List<T>`. Both
     * have the same classifier and arguments, so only equality tells them apart. A platform type from Java,
     * `(Mutable)List<T>!`, carries the mutable mark too, so the check compares with a mutable type rebuilt from the
     * classifier and arguments rather than asking whether [type] is marked: the platform type equals neither, and is
     * read-only here.
     */
    private fun isMutableCollection(type: KType, jvmClass: Class<*>): Boolean =
        type == Reflection.mutableCollectionType(rebuild(type, jvmClass))

    /** Whether [type], whose classifier is `java.lang.Void`, is `Nothing` or `Nothing?`, which `typeOf` marks so. */
    private fun isNothing(type: KType): Boolean = type == Reflection.nothingType(rebuild(type, Void::class.java))

    private fun rebuild(type: KType, jvmClass: Class<*>): KType {
        val arguments = type.arguments.toTypedArray()
        return if (type.isMarkedNullable) {
            Reflection.nullableTypeOf(jvmClass, *arguments)
        } else {
            Reflection.typeOf(jvmClass, *arguments)
        }
    }

    private fun collectionKind(jvmClass: Class<*>, mutable: Boolean): CollectionKind? = when {
        jvmClass.isArray -> if (jvmClass.componentType.isPrimitive) CollectionKind.PRIMITIVE_ARRAY else CollectionKind.ARRAY
        jvmClass == List::class.java -> if (mutable) CollectionKind.MUTABLE_LIST else CollectionKind.LIST
        jvmClass == Map::class.java -> if (mutable) CollectionKind.MUTABLE_MAP else CollectionKind.MAP
        jvmClass == Set::class.java -> if (mutable) CollectionKind.MUTABLE_SET else CollectionKind.SET
        jvmClass == Collection::class.java -> if (mutable) CollectionKind.MUTABLE_COLLECTION else CollectionKind.COLLECTION
        jvmClass == Iterable::class.java -> if (mutable) CollectionKind.MUTABLE_ITERABLE else CollectionKind.ITERABLE
        else -> null
    }

    private companion object {
        val BUILT_INS: Map<Class<*>, BuiltInKind> = BuiltInKind.entries.associateBy { it.jvmClass }

        /** The Kotlin names of the mutable views of the Java interfaces that Kotlin maps to read-only ones. */
        val MUTABLE_NAMES: Map<Class<*>, String> = mapOf(
            Iterable::class.java to "kotlin.collections.MutableIterable",
            Iterator::class.java to "kotlin.collections.MutableIterator",
            Collection::class.java to "kotlin.collections.MutableCollection",
            List::class.java to "kotlin.collections.MutableList",
            ListIterator::class.java to "kotlin.collections.MutableListIterator",
            Set::class.java to "kotlin.collections.MutableSet",
            Map::class.java to "kotlin.collections.MutableMap",
            Map.Entry::class.java to "kotlin.collections.MutableMap.MutableEntry",
        )
    }
}
