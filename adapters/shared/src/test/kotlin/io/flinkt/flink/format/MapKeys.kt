package io.flinkt.flink.format

import io.flinkt.schema.DeclarationSchema
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.schema.SchemaTypeArgument
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.ValueClassSchema

/**
 * Which keys of a `Map` a codec counts toward [RecordFormat.MAX_KEYS_PER_HASH_CODE] (docs/persisted-format.md, "Keys
 * that share a hash code"), decided from the key type, with its type arguments, when the codec is created. KSP and
 * `typeInfo` decide the other `Map` key rules there too (docs/architecture.md, "Map keys"). It's evidence for #5, not
 * production code.
 *
 * A writer counts keys by the hash codes of the JVM that writes the map, and a reader by those of the JVM that reads
 * it, so a key's hash code has to be the same in both, or a map the writer accepted could fail its read. A record's
 * or a value class's generated `hashCode()` combines its components' hash codes, which are specified for the
 * built-ins. An enum constant's, and a plain object's, is an identity hash code, which depends on what the JVM did
 * before. So inside a record or a value class that's part of a key, an enum, and a sealed type with an object subtype,
 * are rejected. An enum constant or an object can still be the key itself, as the key type or a subtype of a sealed
 * key type. A map holds each such key at most once, so it isn't counted.
 */
object MapKeys {
    enum class Counting {
        /**
         * A built-in or an enum, with or without `?`: no key is counted. `HashMap` orders a built-in's keys that share
         * a hash code by `compareTo`, because each built-in class is `Comparable` to itself, and an enum has only as
         * many keys as it has constants.
         */
        NONE,

        /**
         * A record, a value class or a sealed type, with or without `?`: each key that's a record or a value class is
         * counted, and an enum constant, an object and `null` aren't.
         */
        RECORDS_AND_VALUE_CLASSES,
    }

    /**
     * How [key]'s values are counted, with each declaration found by [declarations]. Fails if [key] holds an enum or
     * an object inside a record or a value class, or holds a collection anywhere.
     */
    fun counting(key: SchemaType.ClassType, declarations: (String) -> DeclarationSchema?): Counting =
        Checker(key, declarations).key(key)

    private class Checker(private val root: SchemaType.ClassType, private val declarations: (String) -> DeclarationSchema?) {
        /** Records, value classes and sealed types, as substituted, already found to hold no identity hash code. */
        private val stable = HashSet<SchemaType.ClassType>()

        /** [type] is the key, or a subtype of a sealed key type, where an enum constant or an object may be. */
        fun key(type: SchemaType.ClassType): Counting = when (val declaration = declarations(type.name)) {
            null -> {
                requireNoCollection(type, root.toString())
                Counting.NONE
            }
            is EnumSchema, is ObjectSchema -> Counting.NONE
            is RecordSchema, is ValueClassSchema -> {
                requireStableMembers(type, declaration, type.name)
                Counting.RECORDS_AND_VALUE_CLASSES
            }
            is SealedSchema -> {
                for (subtype in subtypes(type, declaration)) key(subtype)
                Counting.RECORDS_AND_VALUE_CLASSES
            }
        }

        /** [type], a component of a record or a value class at [path], has a hash code that's the same in every JVM. */
        private fun requireStable(type: SchemaType, path: String) {
            type as SchemaType.ClassType
            when (val declaration = declarations(type.name)) {
                null -> requireNoCollection(type, path)
                is EnumSchema -> throw identityHash("the enum ${type.name}", path, "An enum constant's")
                is ObjectSchema -> throw identityHash("the object ${type.name}", path, "An object's")
                is RecordSchema, is ValueClassSchema ->
                    if (stable.add(nonNull(type))) requireStableMembers(type, declaration, path)
                is SealedSchema -> if (stable.add(nonNull(type))) {
                    for (subtype in subtypes(type, declaration)) {
                        if (declarations(subtype.name) is ObjectSchema) {
                            throw identityHash("${subtype.name}, an object subtype of ${type.name},", path, "An object's")
                        }
                        requireStable(subtype, path)
                    }
                }
            }
        }

        private fun requireStableMembers(type: SchemaType.ClassType, declaration: DeclarationSchema, path: String) {
            val bindings = declaration.typeParameters.zip(type.arguments.map { checkNotNull(it.type) }).toMap()
            val members = when (declaration) {
                is RecordSchema -> declaration.fields
                is ValueClassSchema -> listOf(declaration.underlying)
                else -> emptyList()
            }
            for (member in members) requireStable(substitute(member.type, bindings), "$path.${member.name}")
        }

        private fun requireNoCollection(type: SchemaType.ClassType, path: String) {
            require(type.name !in COLLECTIONS) {
                "$root can't be a Map key: it holds a ${type.name} at $path, and no key type holds a collection"
            }
        }

        private fun identityHash(what: String, path: String, whose: String) = IllegalArgumentException(
            "$root can't be a Map key: it holds $what at $path. $whose hashCode() is an identity hash code, which " +
                "another JVM can compute differently, and a map's keys are counted by hash code where it's written " +
                "and where it's read. An enum or an object may be the key type itself, or a subtype of a sealed key type",
        )

        private fun subtypes(type: SchemaType.ClassType, declaration: SealedSchema): List<SchemaType.ClassType> {
            val bindings = declaration.typeParameters.zip(type.arguments.map { checkNotNull(it.type) }).toMap()
            return declaration.subtypes.map { substitute(it.type, bindings) as SchemaType.ClassType }
        }

        private fun nonNull(type: SchemaType.ClassType) =
            if (type.isNullable) SchemaType.ClassType(type.name, type.arguments) else type

        private fun substitute(type: SchemaType, bindings: Map<String, SchemaType>): SchemaType = when (type) {
            is SchemaType.TypeParameter -> {
                val argument = bindings.getValue(type.name)
                if (type.isNullable && !argument.isNullable && argument is SchemaType.ClassType) {
                    SchemaType.ClassType(argument.name, argument.arguments, isNullable = true)
                } else {
                    argument
                }
            }
            is SchemaType.ClassType -> SchemaType.ClassType(
                type.name,
                type.arguments.map { SchemaTypeArgument(it.variance, it.type?.let { t -> substitute(t, bindings) }) },
                type.isNullable,
            )
        }
    }

    private val COLLECTIONS = setOf("kotlin.collections.List", "kotlin.collections.Map")
}
