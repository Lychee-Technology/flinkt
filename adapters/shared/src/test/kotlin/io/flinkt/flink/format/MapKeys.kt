package io.flinkt.flink.format

import io.flinkt.schema.DeclarationSchema
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
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
 *
 * The check never substitutes a type argument into a declaration. Each declaration is checked once, as a function of
 * its type arguments: what it holds whatever they are, and which of its type parameters' arguments are components of
 * it. A use of the declaration then checks only those arguments, where they're written. So the work is linear in the
 * schema's size however many instantiations a key's value can hold, as the snapshot's nesting check is
 * (docs/persisted-format.md, "Validation and budgets").
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
    fun counting(key: SchemaType.ClassType, declarations: (String) -> DeclarationSchema?): Counting {
        Checker(key, declarations).check()
        return when (declarations(key.name)) {
            null, is EnumSchema, is ObjectSchema -> Counting.NONE
            is RecordSchema, is ValueClassSchema, is SealedSchema -> Counting.RECORDS_AND_VALUE_CLASSES
        }
    }

    /**
     * The records of [schema] whose generated `hashCode()` a codec may count: every record that a `Map`'s key type
     * holds, as the key or at any depth inside it. A constructor reorder of one changes its keys' hash codes, so it
     * isn't as-is ([Planner]).
     *
     * A type parameter that holds part of a key in one use of its declaration counts the arguments of every use. For
     * a generic declaration that's an over-approximation, which may name a record no key holds and never leaves one
     * out. It's linear in the schema's size: each declaration's body is walked once, and each type argument once per
     * type parameter it's for.
     */
    fun countedRecords(schema: PersistedSchema): Set<String> = CountedRecords(schema).find()

    /** A path from the key to a part of its value, joined only when a failure names it. */
    private sealed class Path {
        object Here : Path()

        class Segment(val name: String) : Path()

        class Joined(val first: Path, val then: Path) : Path()

        override fun toString(): String {
            val segments = ArrayList<String>()
            val pending = ArrayDeque<Path>().apply { add(this@Path) }
            while (pending.isNotEmpty()) {
                when (val path = pending.removeFirst()) {
                    Here -> Unit
                    is Segment -> segments += path.name
                    is Joined -> {
                        pending.addFirst(path.then)
                        pending.addFirst(path.first)
                    }
                }
            }
            return segments.joinToString(".")
        }

        companion object {
            fun join(first: Path, then: Path): Path = when {
                first === Here -> then
                then === Here -> first
                else -> Joined(first, then)
            }
        }
    }

    /** Why a key type is rejected, at a [path] relative to the value it was found in. */
    private class Failure(val path: Path, private val message: (root: String, path: String) -> String) {
        fun under(prefix: Path) = Failure(Path.join(prefix, path), message)

        fun exception(root: SchemaType.ClassType) = IllegalArgumentException(message(root.toString(), path.toString()))
    }

    /**
     * What a declaration's value requires, whatever its type arguments: a [failure] it holds in any case, or else the
     * type parameters whose arguments are components of it, each with the first path to such a component.
     */
    private class Summary(val failure: Failure?, val parameters: Map<Int, Path>)

    /** Where a value is: the key itself, or a sealed key type's subtype, or a component of a record or a value class. */
    private enum class Position { KEY, COMPONENT }

    private class Checker(private val root: SchemaType.ClassType, private val declarations: (String) -> DeclarationSchema?) {
        private val asComponent = HashMap<String, Summary>()
        private val asKey = HashMap<String, Summary>()
        private val inProgress = HashSet<String>()

        fun check() {
            use(root, Position.KEY, Path.Here, parameters = emptyMap(), uses = LinkedHashMap(), depth = 1)?.let { throw it.exception(root) }
        }

        /**
         * Checks [type], written in a body whose type parameters are [parameters], at [position] and [path]. Returns a
         * failure it holds whatever that body's arguments are, and adds to [uses] each of the body's type parameters
         * whose argument is a component, with its path.
         */
        private fun use(
            type: SchemaType,
            position: Position,
            path: Path,
            parameters: Map<String, Int>,
            uses: MutableMap<Int, Path>,
            depth: Int,
        ): Failure? {
            // A schema the snapshot's validation accepted nests at most MAX_DEPTH levels, and so does this walk on it.
            require(depth <= 2 * PersistedSchema.MAX_DEPTH) { "$root nests deeper than ${PersistedSchema.MAX_DEPTH} levels" }
            if (type is SchemaType.TypeParameter) {
                uses.putIfAbsent(parameters.getValue(type.name), path)
                return null
            }
            type as SchemaType.ClassType
            val summary: Summary
            val base: Path
            when (val declaration = declarations(type.name)) {
                null -> return if (type.name !in COLLECTIONS) {
                    null
                } else {
                    Failure(if (position == Position.KEY) Path.Segment(root.toString()) else path) { root, at ->
                        "$root can't be a Map key: it holds a ${type.name} at $at, and no key type holds a collection"
                    }
                }
                is EnumSchema -> return if (position == Position.KEY) null else identityHash(path, "the enum ${type.name}", "An enum constant's")
                is ObjectSchema -> return if (position == Position.KEY) null else identityHash(path, "the object ${type.name}", "An object's")
                is RecordSchema, is ValueClassSchema -> {
                    summary = componentSummary(declaration, depth + 1)
                    // The key, or a sealed key type's subtype, is where a path starts: at its type name.
                    base = if (position == Position.KEY) Path.join(path, Path.Segment(type.name)) else path
                }
                is SealedSchema -> {
                    summary = if (position == Position.KEY) keySummary(declaration, depth + 1) else componentSummary(declaration, depth + 1)
                    base = path
                }
            }
            summary.failure?.let { return it.under(base) }
            for ((index, inner) in summary.parameters) {
                val argument = checkNotNull(type.arguments[index].type) { "$type has a star projection" }
                use(argument, Position.COMPONENT, Path.join(base, inner), parameters, uses, depth + 1)?.let { return it }
            }
            return null
        }

        /** [declaration]'s value as a component of a key: each member is a component too. */
        private fun componentSummary(declaration: DeclarationSchema, depth: Int): Summary =
            asComponent[declaration.typeName] ?: summarize(declaration) { parameters, uses ->
                when (declaration) {
                    is RecordSchema -> declaration.fields.firstNotNullOfOrNull { field ->
                        use(field.type, Position.COMPONENT, Path.Segment(field.name), parameters, uses, depth)
                    }
                    is ValueClassSchema ->
                        use(declaration.underlying.type, Position.COMPONENT, Path.Segment(declaration.underlying.name), parameters, uses, depth)
                    is SealedSchema -> declaration.subtypes.firstNotNullOfOrNull { subtype ->
                        if (declarations(subtype.type.name) is ObjectSchema) {
                            identityHash(Path.Here, "${subtype.type.name}, an object subtype of ${declaration.typeName},", "An object's")
                        } else {
                            use(subtype.type, Position.COMPONENT, Path.Here, parameters, uses, depth)
                        }
                    }
                    is EnumSchema, is ObjectSchema -> null
                }
            }.also { asComponent[declaration.typeName] = it }

        /** A sealed key type's value: each subtype is in the key's position, where an object may be. */
        private fun keySummary(declaration: SealedSchema, depth: Int): Summary =
            asKey[declaration.typeName] ?: summarize(declaration) { parameters, uses ->
                declaration.subtypes.firstNotNullOfOrNull { subtype -> use(subtype.type, Position.KEY, Path.Here, parameters, uses, depth) }
            }.also { asKey[declaration.typeName] = it }

        private inline fun summarize(
            declaration: DeclarationSchema,
            body: (Map<String, Int>, MutableMap<Int, Path>) -> Failure?,
        ): Summary {
            val name = declaration.typeName
            require(inProgress.add(name)) { "$name holds itself: recursive types aren't supported in this release" }
            val parameters = HashMap<String, Int>()
            declaration.typeParameters.forEachIndexed { index, parameter -> parameters[parameter] = index }
            val uses = LinkedHashMap<Int, Path>()
            val failure = body(parameters, uses)
            inProgress.remove(name)
            return Summary(failure, if (failure == null) uses else emptyMap())
        }

        private fun identityHash(path: Path, what: String, whose: String) = Failure(path) { root, at ->
            "$root can't be a Map key: it holds $what at $at. $whose hashCode() is an identity hash code, which " +
                "another JVM can compute differently, and a map's keys are counted by hash code where it's written " +
                "and where it's read. An enum or an object may be the key type itself, or a subtype of a sealed key type"
        }
    }

    private class CountedRecords(private val schema: PersistedSchema) {
        /** Every class type of the schema that names a declaration, with the declaration it's written in. */
        private val uses = HashMap<String, MutableList<Pair<SchemaType.ClassType, DeclarationSchema?>>>()
        private val maps = ArrayList<Pair<SchemaType.ClassType, DeclarationSchema?>>()
        private val keyed = HashSet<String>()
        private val keyedParameters = HashSet<Pair<String, Int>>()
        private val pending = ArrayDeque<() -> Unit>()

        /** Each declaration's type parameters' positions, built once, so a wide declaration costs no search per use. */
        private val positions = HashMap<String, Map<String, Int>>()

        fun find(): Set<String> {
            index(schema.root, scope = null)
            for (declaration in schema.declarations.values) {
                PersistedSchema.memberTypes(declaration).forEach { index(it, declaration) }
            }
            for ((map, scope) in maps) mark(checkNotNull(map.arguments[0].type), scope)
            while (pending.isNotEmpty()) pending.removeFirst()()
            return keyed.filterTo(HashSet()) { schema.declarations[it] is RecordSchema }
        }

        private fun index(type: SchemaType, scope: DeclarationSchema?) {
            if (type !is SchemaType.ClassType) return
            if (type.name == MAP) maps += type to scope
            if (type.name in schema.declarations) uses.getOrPut(type.name) { ArrayList() } += type to scope
            type.arguments.forEach { argument -> argument.type?.let { index(it, scope) } }
        }

        /** [type], written in [scope], is part of a key: so is every declaration it names, and each type parameter's argument. */
        private fun mark(type: SchemaType, scope: DeclarationSchema?) {
            when (type) {
                is SchemaType.TypeParameter -> {
                    val declaration = checkNotNull(scope) { "${type.name} is used outside a generic declaration" }
                    val position = positions.getOrPut(declaration.typeName) {
                        HashMap<String, Int>().also { map -> declaration.typeParameters.forEachIndexed { index, name -> map[name] = index } }
                    }
                    val parameter = declaration.typeName to position.getValue(type.name)
                    if (keyedParameters.add(parameter)) {
                        pending += {
                            for ((use, outer) in uses[parameter.first].orEmpty()) mark(checkNotNull(use.arguments[parameter.second].type), outer)
                        }
                    }
                }
                is SchemaType.ClassType -> {
                    val declaration = schema.declarations[type.name]
                    if (declaration != null && keyed.add(type.name)) {
                        pending += { PersistedSchema.memberTypes(declaration).forEach { mark(it, declaration) } }
                    }
                    type.arguments.forEach { argument -> argument.type?.let { mark(it, scope) } }
                }
            }
        }
    }

    private const val MAP = "kotlin.collections.Map"
    private val COLLECTIONS = setOf("kotlin.collections.List", MAP)
}
