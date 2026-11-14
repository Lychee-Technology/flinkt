package io.flinkt.flink.format

import io.flinkt.schema.DeclarationSchema
import io.flinkt.schema.EnumConstantSchema
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.schema.SchemaTypeArgument
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.SealedSubtypeSchema
import io.flinkt.schema.ValueClassSchema
import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataOutput
import java.io.DataOutputStream
import kotlin.reflect.KVariance

/** The key role a serializer was created in (docs/schema-evolution.md, "Keys"), as the snapshot records it. */
enum class KeyRole(val code: Int) {
    VALUE(0),
    MAP_KEY(1),
    PARTITION_KEY(2),
}

/**
 * A prototype of snapshot format version 1 (docs/persisted-format.md, "Snapshot"): the Flink-free part of the data
 * Flinkt's one snapshot class writes. It records the record format version, the key role, the serialized type with
 * its arguments as substituted, and every declaration reachable from it, each once, sorted by name and referenced by
 * name.
 *
 * Building one ([of]) and reading one ([read]) run the same validation and the same budgets, so a schema this
 * release can write is one it can read, and a reader never rejects a snapshot its own release could have written.
 *
 * It's evidence for #5. #12 implements the format in `flinkt-core`, on the core schema model, as this does.
 */
class PersistedSchema private constructor(
    val recordFormatVersion: Int,
    val role: KeyRole,
    /** The serialized type, with its type arguments as substituted. It refers to no type parameter. */
    val root: SchemaType.ClassType,
    /** Every declaration reachable from [root], by type name, in ascending `String.compareTo` order of the names. */
    val declarations: Map<String, DeclarationSchema>,
) {
    /** The declaration of a type that isn't a built-in or a collection. */
    fun declaration(name: String): DeclarationSchema =
        declarations[name] ?: throw IllegalArgumentException("The snapshot has no declaration of $name")

    /** Writes the snapshot data of format version 1. */
    fun write(output: DataOutput) {
        output.write(encoded)
    }

    /** The encoded bytes, which [of] checks against [MAX_SNAPSHOT_BYTES] before anything is written. */
    val encoded: ByteArray by lazy {
        val bytes = ByteArrayOutputStream()
        Writer(DataOutputStream(bytes)).writeSchema(this)
        bytes.toByteArray()
    }

    /**
     * Whether [other] describes the same persisted layout with the same identities: the same record format version,
     * root and declarations. The role isn't compared, because it says what the serializer was used for, not how its
     * bytes look. Constructor order is compared only when [compareConstructorOrder] is set: it doesn't decide bytes.
     */
    fun sameLayout(other: PersistedSchema, compareConstructorOrder: Boolean): Boolean =
        recordFormatVersion == other.recordFormatVersion &&
            root == other.root &&
            declarations.keys == other.declarations.keys &&
            declarations.all { (name, declaration) ->
                sameDeclaration(declaration, other.declarations.getValue(name), compareConstructorOrder)
            }

    override fun toString(): String =
        "PersistedSchema(record format $recordFormatVersion, $role, $root, ${declarations.values.joinToString()})"

    companion object {
        /** The snapshot format version this prototype writes, and the only one it reads. */
        const val SNAPSHOT_FORMAT_VERSION: Int = 1

        /** The v1 built-ins, which a snapshot names without a declaration. */
        val BUILT_INS: Set<String> = setOf(
            "kotlin.Boolean", "kotlin.Byte", "kotlin.Short", "kotlin.Int", "kotlin.Long",
            "kotlin.Float", "kotlin.Double", "kotlin.Char", "kotlin.String",
        )

        /** The v1 collections (#16), with the number of type arguments each takes. */
        val COLLECTIONS: Map<String, Int> = mapOf("kotlin.collections.List" to 1, "kotlin.collections.Map" to 2)

        /** The budget on the snapshot data's size, in bytes. */
        const val MAX_SNAPSHOT_BYTES: Int = 16 * 1024 * 1024

        /** The budget on a name's length, in UTF-16 chars: a JVM class file can't hold a longer class or field name. */
        const val MAX_NAME_LENGTH: Int = 65_535

        /**
         * The budget on nesting: how deep reading a value recurses through records, collections and type arguments.
         * Every check of a schema stops at this depth too, so none recurses deeper, however many declarations follow.
         */
        const val MAX_DEPTH: Int = 64

        /** The reserved field attribute for #35's dropped-value marker. Critical: a reader that doesn't know it fails. */
        const val DROPPED_VALUE_ATTRIBUTE: Int = 1

        /**
         * Builds the schema of [root] in [role], with the declaration of every type reachable from it, which
         * [declarationOf] looks up by name, as a registry does through the job's class loader.
         *
         * Enum constants and sealed subtypes are kept in ascending ID order, so a source reorder changes neither the
         * records nor the snapshot. A record's fields stay in constructor order, which the snapshot records.
         */
        fun of(
            root: SchemaType.ClassType,
            role: KeyRole = KeyRole.VALUE,
            declarationOf: (String) -> DeclarationSchema?,
        ): PersistedSchema {
            val declarations = sortedMapOf<String, DeclarationSchema>()
            // Declarations are found through a queue, so a long chain of them doesn't recurse. A type's arguments
            // recurse, as deep as the nesting budget.
            val pending = ArrayDeque<String>()
            fun visit(type: SchemaType?, nesting: Int) {
                require(nesting <= MAX_DEPTH) { "a type nests deeper than $MAX_DEPTH levels" }
                when (type) {
                    null, is SchemaType.TypeParameter -> Unit
                    is SchemaType.ClassType -> {
                        if (type.name !in BUILT_INS && type.name !in COLLECTIONS && type.name !in declarations) {
                            val declaration = declarationOf(type.name)
                                ?: throw IllegalArgumentException("No declaration of ${type.name} was found")
                            declarations[type.name] = canonical(declaration)
                            pending += type.name
                        }
                        type.arguments.forEach { visit(it.type, nesting + 1) }
                    }
                }
            }
            visit(root, nesting = 1)
            while (pending.isNotEmpty()) {
                memberTypes(declarations.getValue(pending.removeFirst())).forEach { visit(it, nesting = 1) }
            }
            val schema = PersistedSchema(RecordFormat.VERSION, role, root, LinkedHashMap(declarations))
            Validator(schema).validate()
            check(schema.encoded.size <= MAX_SNAPSHOT_BYTES) {
                "The snapshot of $root would be ${schema.encoded.size} bytes, over the budget of $MAX_SNAPSHOT_BYTES"
            }
            return schema
        }

        /**
         * Reads snapshot data of format version 1. Malformed data, a budget exceeded, and a schema the validation
         * rejects fail with [MalformedDataException], and a truncated one with `EOFException`.
         */
        fun read(input: DataInput): PersistedSchema {
            val reader = Reader(BudgetedInput(input, MAX_SNAPSHOT_BYTES))
            val schema = try {
                reader.readSchema()
            } catch (e: IllegalArgumentException) {
                // The schema model's own checks, such as two fields of one record with the same name.
                throw MalformedDataException("the snapshot holds an invalid schema: ${e.message}")
            }
            try {
                Validator(schema).validate()
            } catch (e: IllegalArgumentException) {
                throw MalformedDataException("the snapshot holds an invalid schema: ${e.message}")
            }
            return schema
        }

        /** The types a declaration's values are made of. */
        fun memberTypes(declaration: DeclarationSchema): List<SchemaType> = when (declaration) {
            is RecordSchema -> declaration.fields.map { it.type }
            is ValueClassSchema -> listOf(declaration.underlying.type)
            is SealedSchema -> declaration.subtypes.map { it.type }
            is EnumSchema, is ObjectSchema -> emptyList()
        }

        private fun canonical(declaration: DeclarationSchema): DeclarationSchema = when (declaration) {
            is EnumSchema -> EnumSchema(declaration.typeName, declaration.constants.sortedBy { it.id })
            is SealedSchema ->
                SealedSchema(declaration.typeName, declaration.subtypes.sortedBy { it.id }, declaration.typeParameters)
            else -> declaration
        }

        private fun sameDeclaration(a: DeclarationSchema, b: DeclarationSchema, compareConstructorOrder: Boolean): Boolean =
            if (a is RecordSchema && b is RecordSchema && !compareConstructorOrder) {
                a.typeName == b.typeName &&
                    a.typeParameters == b.typeParameters &&
                    a.fields.sortedBy { it.name } == b.fields.sortedBy { it.name }
            } else {
                a == b
            }
    }

    private class Writer(private val output: DataOutput) {
        fun writeSchema(schema: PersistedSchema) {
            output.writeInt(schema.recordFormatVersion)
            output.writeByte(schema.role.code)
            writeType(schema.root)
            output.writeInt(schema.declarations.size)
            schema.declarations.values.forEach(::writeDeclaration)
            writeNoAttributes()
        }

        private fun writeDeclaration(declaration: DeclarationSchema) {
            output.writeByte(declarationKind(declaration))
            RecordFormat.writeString(output, declaration.typeName)
            when (declaration) {
                is RecordSchema -> {
                    writeNames(declaration.typeParameters)
                    output.writeInt(declaration.fields.size)
                    declaration.fields.forEach(::writeField)
                }
                is ValueClassSchema -> {
                    writeNames(declaration.typeParameters)
                    writeField(declaration.underlying)
                }
                is EnumSchema -> {
                    output.writeInt(declaration.constants.size)
                    for (constant in declaration.constants) {
                        output.writeInt(constant.id)
                        RecordFormat.writeString(output, constant.name)
                    }
                }
                is SealedSchema -> {
                    writeNames(declaration.typeParameters)
                    output.writeInt(declaration.subtypes.size)
                    for (subtype in declaration.subtypes) {
                        output.writeInt(subtype.id)
                        writeType(subtype.type)
                    }
                }
                is ObjectSchema -> Unit
            }
            writeNoAttributes()
        }

        private fun writeField(field: FieldSchema) {
            RecordFormat.writeString(output, field.name)
            writeType(field.type)
            writeNoAttributes()
        }

        private fun writeType(type: SchemaType) {
            when (type) {
                is SchemaType.ClassType -> {
                    output.writeByte(CLASS_TYPE)
                    RecordFormat.writeString(output, type.name)
                    RecordFormat.writeBoolean(output, type.isNullable)
                    output.writeInt(type.arguments.size)
                    for (argument in type.arguments) {
                        output.writeByte(varianceCode(argument.variance))
                        argument.type?.let(::writeType)
                    }
                }
                is SchemaType.TypeParameter -> {
                    output.writeByte(TYPE_PARAMETER)
                    RecordFormat.writeString(output, type.name)
                    RecordFormat.writeBoolean(output, type.isNullable)
                }
            }
        }

        private fun writeNames(names: List<String>) {
            output.writeInt(names.size)
            names.forEach { RecordFormat.writeString(output, it) }
        }

        // This release defines no attributes. #35's dropped-value marker would be a field attribute.
        private fun writeNoAttributes() {
            output.writeInt(0)
        }
    }

    private class Reader(private val input: BudgetedInput) {
        fun readSchema(): PersistedSchema {
            val recordFormatVersion = input.readInt()
            if (recordFormatVersion < 1) throw MalformedDataException("record format version $recordFormatVersion")
            val roleCode = input.readUnsignedByte()
            val role = KeyRole.entries.firstOrNull { it.code == roleCode }
                ?: throw MalformedDataException("unknown key role $roleCode")
            val root = readType(depth = 1) as? SchemaType.ClassType
                ?: throw MalformedDataException("the serialized type is a type parameter")
            val declarations = LinkedHashMap<String, DeclarationSchema>()
            var previous: String? = null
            repeat(readCount("declarations")) {
                val declaration = readDeclaration()
                val name = declaration.typeName
                if (previous != null && previous!! >= name) {
                    throw MalformedDataException("declaration $name follows $previous: names must be in ascending order, each once")
                }
                previous = name
                declarations[name] = declaration
            }
            readAttributes("the snapshot")
            return PersistedSchema(recordFormatVersion, role, root, declarations)
        }

        private fun readDeclaration(): DeclarationSchema {
            val kind = input.readUnsignedByte()
            val name = readName("a declaration name")
            val declaration = when (kind) {
                RECORD -> {
                    val typeParameters = readNames(name)
                    val fields = readList("fields of $name") { readField(name) }
                    RecordSchema(name, fields, typeParameters)
                }
                VALUE_CLASS -> {
                    val typeParameters = readNames(name)
                    ValueClassSchema(name, readField(name), typeParameters)
                }
                ENUM -> {
                    var previousId = 0
                    val constants = readList("constants of $name") {
                        val id = input.readInt()
                        if (id <= previousId) {
                            throw MalformedDataException("$name: constant ID $id follows $previousId: IDs are positive and ascending")
                        }
                        previousId = id
                        EnumConstantSchema(readName("a constant of $name"), id)
                    }
                    EnumSchema(name, constants)
                }
                SEALED -> {
                    val typeParameters = readNames(name)
                    var previousId = 0
                    val subtypes = readList("subtypes of $name") {
                        val id = input.readInt()
                        if (id <= previousId) {
                            throw MalformedDataException("$name: subtype ID $id follows $previousId: IDs are positive and ascending")
                        }
                        previousId = id
                        val type = readType(depth = 1) as? SchemaType.ClassType
                            ?: throw MalformedDataException("$name: subtype $id is a type parameter")
                        SealedSubtypeSchema(id, type)
                    }
                    SealedSchema(name, subtypes, typeParameters)
                }
                OBJECT -> ObjectSchema(name)
                else -> throw MalformedDataException(
                    "$name has declaration kind $kind, which snapshot format version $SNAPSHOT_FORMAT_VERSION doesn't define",
                )
            }
            readAttributes(name)
            return declaration
        }

        private fun readField(declaration: String): FieldSchema {
            val name = readName("a field name of $declaration")
            val field = FieldSchema(name, readType(depth = 1))
            readAttributes("$declaration.$name")
            return field
        }

        private fun readType(depth: Int): SchemaType {
            if (depth > MAX_DEPTH) throw MalformedDataException("a type nests deeper than $MAX_DEPTH levels")
            return when (val kind = input.readUnsignedByte()) {
                CLASS_TYPE -> {
                    val name = readName("a type name")
                    val nullable = RecordFormat.readBoolean(input)
                    val arguments = readList("type arguments of $name") {
                        when (val variance = input.readUnsignedByte()) {
                            STAR -> SchemaTypeArgument.STAR
                            INVARIANT, IN, OUT -> SchemaTypeArgument(variance(variance), readType(depth + 1))
                            else -> throw MalformedDataException("unknown variance $variance in a type argument of $name")
                        }
                    }
                    SchemaType.ClassType(name, arguments, nullable)
                }
                TYPE_PARAMETER -> SchemaType.TypeParameter(readName("a type parameter"), RecordFormat.readBoolean(input))
                else -> throw MalformedDataException(
                    "type kind $kind, which snapshot format version $SNAPSHOT_FORMAT_VERSION doesn't define",
                )
            }
        }

        private fun readNames(declaration: String): List<String> =
            readList("type parameters of $declaration") { readName("a type parameter of $declaration") }

        // A name over its budget fails at its length, before a char is read.
        private fun readName(what: String): String {
            val name = RecordFormat.readString(input, MAX_NAME_LENGTH, what)
            if (name.isEmpty()) throw MalformedDataException("$what is empty")
            return name
        }

        // Nothing is allocated from a count: each element is read before it's added, and each takes at least one
        // byte, so the size budget bounds every list.
        private fun readCount(what: String): Int {
            val count = input.readInt()
            if (count < 0) throw MalformedDataException("$what: a count of $count")
            if (count > input.remaining) {
                throw MalformedDataException("$what: a count of $count can't fit in the ${input.remaining} bytes left of the budget")
            }
            return count
        }

        private inline fun <T> readList(what: String, element: () -> T): List<T> {
            val count = readCount(what)
            val list = ArrayList<T>(minOf(count, 16))
            repeat(count) { list += element() }
            return list
        }

        private fun readAttributes(owner: String) {
            var previousTag = 0
            repeat(readCount("attributes of $owner")) {
                val tag = input.readInt()
                if (tag <= previousTag) {
                    throw MalformedDataException("$owner: attribute $tag follows $previousTag: tags are positive and ascending")
                }
                previousTag = tag
                val critical = RecordFormat.readBoolean(input)
                val length = readCount("attribute $tag of $owner")
                // This release knows no attribute. A non-critical one is information an older reader may ignore, and
                // a critical one changes how the snapshot must be read, so an older reader fails on it.
                if (critical) {
                    throw MalformedDataException(
                        "$owner has critical attribute $tag, which this reader of snapshot format version " +
                            "$SNAPSHOT_FORMAT_VERSION doesn't know",
                    )
                }
                input.skipFully(length)
            }
        }
    }

    /**
     * The checks [of] and [read] share. Each failure is an `IllegalArgumentException` naming what's wrong.
     *
     * Each check is bounded whatever the schema holds: none recurses deeper than [MAX_DEPTH] levels, each declaration
     * is walked once, and the work is linear in the schema's size, so a snapshot that the size budget admits fails
     * with a diagnostic, not with a `StackOverflowError` or after a long walk.
     */
    private class Validator(private val schema: PersistedSchema) {
        fun validate() {
            for (name in schema.declarations.keys) checkName(name)
            checkType(schema.root, scope = null, parameters = emptySet(), takesBytes = false, nesting = 1)
            for (declaration in schema.declarations.values) {
                checkDeclaration(declaration)
                val parameters = declaration.typeParameters.toHashSet()
                // A subtype's payload follows its ID, so the subtype itself may take no more bytes, as an object does.
                val takesBytes = declaration !is SealedSchema
                for (type in memberTypes(declaration)) checkType(type, declaration, parameters, takesBytes, nesting = 1)
            }
            Nesting().check()
        }

        private fun checkName(name: String) {
            require(name.isNotEmpty() && name.length <= MAX_NAME_LENGTH) {
                "a name is ${name.length} chars long, and names are 1 to $MAX_NAME_LENGTH chars"
            }
        }

        private fun checkDeclaration(declaration: DeclarationSchema) {
            declaration.typeParameters.forEach(::checkName)
            when (declaration) {
                is RecordSchema -> declaration.fields.forEach { checkName(it.name) }
                is ValueClassSchema -> checkName(declaration.underlying.name)
                is EnumSchema -> {
                    declaration.constants.forEach { checkName(it.name) }
                    val names = declaration.constants.map { it.name }
                    require(names.size == names.toSet().size) { "${declaration.typeName} has two constants with one name" }
                }
                is SealedSchema -> for (subtype in declaration.subtypes) {
                    val target = schema.declarations[subtype.type.name]
                    require(target is RecordSchema || target is ObjectSchema || target is SealedSchema) {
                        "${declaration.typeName}'s subtype ${subtype.type.name} isn't a declared record, object or sealed type"
                    }
                }
                is ObjectSchema -> Unit
            }
        }

        /**
         * [type] is well-formed where it's used: in [scope]'s body, whose type parameters are [parameters], or at the
         * root when [scope] is `null`. When [takesBytes] is set, its values take at least one byte: it's a field, an
         * element, a key, a value or a type argument, not the serialized type or a sealed subtype.
         */
        private fun checkType(
            type: SchemaType,
            scope: DeclarationSchema?,
            parameters: Set<String>,
            takesBytes: Boolean,
            nesting: Int,
        ) {
            require(nesting <= MAX_DEPTH) { "a type nests deeper than $MAX_DEPTH levels" }
            when (type) {
                is SchemaType.TypeParameter -> require(type.name in parameters) {
                    "${type.name} isn't a type parameter of ${scope?.typeName ?: "the serialized type"}"
                }
                is SchemaType.ClassType -> {
                    checkName(type.name)
                    val declaration = schema.declarations[type.name]
                    val expected = when (type.name) {
                        in BUILT_INS -> 0
                        in COLLECTIONS -> COLLECTIONS.getValue(type.name)
                        else -> declaration?.typeParameters?.size
                            ?: throw IllegalArgumentException(
                                "${type.name} is neither a built-in, a collection nor a declaration in the snapshot",
                            )
                    }
                    require(type.arguments.size == expected) {
                        "$type has ${type.arguments.size} type arguments, and ${type.name} takes $expected"
                    }
                    // Every element of a collection then takes at least one byte, so a reader that loops over a size
                    // consumes input on each turn, and so does every other value but the root's and a subtype's.
                    require(!takesBytes || !writtenAsNoBytes(declaration)) {
                        "${scope?.typeName ?: "the serialized type"} uses ${type.name} as a field, an element or a " +
                            "type argument, and its values are written as no bytes. Only the serialized type or a " +
                            "sealed subtype may be, so that reading every other value consumes input"
                    }
                    for (argument in type.arguments) {
                        val argumentType = argument.type
                            ?: throw IllegalArgumentException("$type has a star projection, which has no persisted type")
                        checkType(argumentType, scope, parameters, takesBytes = true, nesting = nesting + 1)
                    }
                }
            }
        }

        /** An object, or a record without fields. Every other value takes a byte, by the rule [checkType] applies. */
        private fun writtenAsNoBytes(declaration: DeclarationSchema?): Boolean =
            declaration is ObjectSchema || declaration is RecordSchema && declaration.fields.isEmpty()

        /**
         * Reachability, cycles and the nesting budget, in one walk from the root that stops at [MAX_DEPTH] levels, so
         * it never recurses deeper, however long a chain of declarations is.
         *
         * Nesting counts one level per record, collection, value class, sealed type or built-in, through type
         * arguments as substituted. A declaration's nesting is a function of its type arguments' nesting:
         * `max(constant, distance(P) + nesting of P's argument, ...)`, over the type parameters `P` its body uses. Each
         * declaration's function is computed once, so a generic schema costs linear work however many instantiations
         * of its declarations a value can hold.
         */
        private inner class Nesting {
            private val bodies = HashMap<String, Body>()
            private val path = ArrayList<String>()
            private val onPath = HashSet<String>()
            private val tooDeep get() = "${schema.root} nests deeper than $MAX_DEPTH levels, over the budget"

            fun check() {
                val depth = of(schema.root, level = 1).constant
                require(depth <= MAX_DEPTH) { tooDeep }
                val unreachable = schema.declarations.keys - bodies.keys
                require(unreachable.isEmpty()) { "the snapshot declares ${unreachable.joinToString()}, which nothing reaches" }
            }

            /**
             * [type]'s nesting, as a function of the type arguments of the declaration it's in. [level] is how deep it
             * is below the root, at least, so past the budget the root nests too deep, whatever the rest holds.
             */
            private fun of(type: SchemaType, level: Int): Depth {
                require(level <= MAX_DEPTH) { tooDeep }
                if (type is SchemaType.TypeParameter) return Depth(NONE, mapOf(type.name to 0))
                type as SchemaType.ClassType
                val arguments = type.arguments.map { of(checkNotNull(it.type), level + 1) }
                val depth = Depth.Builder()
                arguments.forEach { depth.add(it, 0) }
                schema.declarations[type.name]?.let { declaration ->
                    val body = body(declaration, level)
                    depth.add(Depth(body.constant, emptyMap()), 0)
                    for (k in arguments.indices) if (body.distances[k] != NONE) depth.add(arguments[k], body.distances[k])
                }
                // Every argument nests at least one level, so a distance of the budget or more exceeds it.
                return depth.build(plus = 1).also { result ->
                    require(result.constant <= MAX_DEPTH && result.distances.values.all { it < MAX_DEPTH }) { tooDeep }
                }
            }

            private fun body(declaration: DeclarationSchema, level: Int): Body {
                val name = declaration.typeName
                bodies[name]?.let { return it }
                require(onPath.add(name)) {
                    "${(path.subList(path.indexOf(name), path.size) + name).joinToString(" -> ")}: recursive types " +
                        "aren't supported in this release"
                }
                path.add(name)
                val depth = Depth.Builder()
                for (member in memberTypes(declaration)) depth.add(of(member, level + 1), 0)
                path.removeAt(path.size - 1)
                onPath.remove(name)
                val nested = depth.build(plus = 0)
                val body = Body(
                    maxOf(nested.constant, 0),
                    IntArray(declaration.typeParameters.size) { nested.distances[declaration.typeParameters[it]] ?: NONE },
                )
                bodies[name] = body
                return body
            }
        }

        /** A declaration body's nesting: [constant], and the distance to each of its type parameters, or [NONE]. */
        private class Body(val constant: Int, val distances: IntArray)
    }

    /** A type's nesting as a function of type parameters' nesting: the max of [constant] and each distance plus it. */
    private class Depth(val constant: Int, val distances: Map<String, Int>) {
        class Builder {
            private var constant = NONE
            private val distances = HashMap<String, Int>()

            fun add(depth: Depth, plus: Int) {
                if (depth.constant != NONE) constant = maxOf(constant, depth.constant + plus)
                for ((parameter, distance) in depth.distances) distances.merge(parameter, distance + plus) { a, b -> maxOf(a, b) }
            }

            /** The depth with [plus] levels on top: one for a class type, none for a declaration's body. */
            fun build(plus: Int): Depth =
                Depth(maxOf(constant, 0) + plus, if (plus == 0) distances else distances.mapValues { it.value + plus })
        }
    }

    /** A [DataInput] that fails once more than [budget] bytes have been read from it. */
    private class BudgetedInput(private val input: DataInput, private val budget: Int) : DataInput by input {
        var read = 0
            private set
        val remaining: Int get() = budget - read

        private fun take(bytes: Int) {
            if (bytes > remaining) {
                throw MalformedDataException("the snapshot data is longer than the budget of $budget bytes")
            }
            read += bytes
        }

        fun skipFully(bytes: Int) {
            take(bytes)
            var left = bytes
            while (left > 0) {
                val skipped = input.skipBytes(left)
                if (skipped <= 0) {
                    input.readByte()
                    left--
                } else {
                    left -= skipped
                }
            }
        }

        override fun readFully(b: ByteArray) {
            take(b.size)
            input.readFully(b)
        }

        override fun readFully(b: ByteArray, off: Int, len: Int) {
            take(len)
            input.readFully(b, off, len)
        }

        override fun skipBytes(n: Int): Int = throw UnsupportedOperationException("use skipFully")

        override fun readBoolean(): Boolean = take(1).let { input.readBoolean() }

        override fun readByte(): Byte = take(1).let { input.readByte() }

        override fun readUnsignedByte(): Int = take(1).let { input.readUnsignedByte() }

        override fun readShort(): Short = take(2).let { input.readShort() }

        override fun readUnsignedShort(): Int = take(2).let { input.readUnsignedShort() }

        override fun readChar(): Char = take(2).let { input.readChar() }

        override fun readInt(): Int = take(4).let { input.readInt() }

        override fun readLong(): Long = take(8).let { input.readLong() }

        override fun readFloat(): Float = take(4).let { input.readFloat() }

        override fun readDouble(): Double = take(8).let { input.readDouble() }

        override fun readLine(): String = throw UnsupportedOperationException()

        override fun readUTF(): String = throw UnsupportedOperationException()
    }
}

/** Substitutes [arguments] for the type parameters in [type]. */
internal fun substitute(type: SchemaType.ClassType, arguments: Map<String, SchemaType>): SchemaType.ClassType =
    if (arguments.isEmpty()) {
        type
    } else {
        SchemaType.ClassType(
            type.name,
            type.arguments.map { argument ->
                when (val argumentType = argument.type) {
                    null -> argument
                    else -> SchemaTypeArgument(argument.variance, substituteType(argumentType, arguments))
                }
            },
            type.isNullable,
        )
    }

internal fun substituteType(type: SchemaType, arguments: Map<String, SchemaType>): SchemaType = when (type) {
    is SchemaType.ClassType -> substitute(type, arguments)
    is SchemaType.TypeParameter -> {
        val argument = arguments[type.name] ?: type
        if (type.isNullable) nullable(argument) else argument
    }
}

private fun nullable(type: SchemaType): SchemaType = when (type) {
    is SchemaType.ClassType -> SchemaType.ClassType(type.name, type.arguments, isNullable = true)
    is SchemaType.TypeParameter -> SchemaType.TypeParameter(type.name, isNullable = true)
}

/** No such nesting: a type parameter has no constant part, and a body may not use a type parameter. */
private const val NONE = -1

private const val CLASS_TYPE = 1
private const val TYPE_PARAMETER = 2

private const val RECORD = 1
private const val VALUE_CLASS = 2
private const val ENUM = 3
private const val SEALED = 4
private const val OBJECT = 5

private const val STAR = 0
private const val INVARIANT = 1
private const val IN = 2
private const val OUT = 3

private fun declarationKind(declaration: DeclarationSchema): Int = when (declaration) {
    is RecordSchema -> RECORD
    is ValueClassSchema -> VALUE_CLASS
    is EnumSchema -> ENUM
    is SealedSchema -> SEALED
    is ObjectSchema -> OBJECT
}

private fun varianceCode(variance: KVariance?): Int = when (variance) {
    null -> STAR
    KVariance.INVARIANT -> INVARIANT
    KVariance.IN -> IN
    KVariance.OUT -> OUT
}

private fun variance(code: Int): KVariance = when (code) {
    INVARIANT -> KVariance.INVARIANT
    IN -> KVariance.IN
    else -> KVariance.OUT
}
