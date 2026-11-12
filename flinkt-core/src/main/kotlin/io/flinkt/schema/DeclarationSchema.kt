package io.flinkt.schema

/**
 * The structure of one generated declaration, as its `GeneratedTypeModule` registers it: what a serializer snapshot
 * needs to describe the declaration, and what a planner and an old-layout reader work on.
 *
 * A schema names types by strings ([SchemaType]), never by `Class`, so one read back from a snapshot can describe a
 * declaration the current classpath doesn't have. A nested declaration is referenced by name, and has a schema of
 * its own. Which of this a snapshot persists, and how, is #5's decision; this model only holds it.
 *
 * Each kind of declaration is a subclass: [RecordSchema], [ValueClassSchema], [EnumSchema], [SealedSchema] and
 * [ObjectSchema].
 */
public sealed class DeclarationSchema {
    /** The declaration's identity: its JVM class name, as `Class.getName()` spells it, such as `com.example.User`. */
    public abstract val typeName: String

    /** The names of the declaration's type parameters, in declaration order. Empty for a non-generic declaration. */
    public abstract val typeParameters: List<String>
}

/**
 * A class whose value is its primary constructor's properties, such as `data class User(val id: Long, val name: String)`.
 */
public class RecordSchema(
    override val typeName: String,
    /**
     * The persisted fields, in the order of the primary constructor's parameters. That order is how a value is
     * constructed, and a data class's generated `hashCode()` follows it. The order fields are written in is #5's.
     */
    fields: List<FieldSchema>,
    typeParameters: List<String> = emptyList(),
) : DeclarationSchema() {
    public val fields: List<FieldSchema> = fields.toList()
    override val typeParameters: List<String> = typeParameters.toList()

    init {
        validate(typeName, this.typeParameters)
        requireUnique(this.fields.map { it.name }) { "$typeName has two fields named $it" }
        this.fields.forEach { requireDeclared(typeName, this.typeParameters, it.type) }
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is RecordSchema &&
            typeName == other.typeName &&
            typeParameters == other.typeParameters &&
            fields == other.fields

    override fun hashCode(): Int = (typeName.hashCode() * 31 + typeParameters.hashCode()) * 31 + fields.hashCode()

    override fun toString(): String = "record ${declaration(typeName, typeParameters)}(${fields.joinToString()})"
}

/** A property of a record or value class: its Kotlin property name and its full type. */
public class FieldSchema(
    public val name: String,
    public val type: SchemaType,
) {
    init {
        require(name.isNotEmpty()) { "A field needs a name" }
    }

    override fun equals(other: Any?): Boolean =
        this === other || other is FieldSchema && name == other.name && type == other.type

    override fun hashCode(): Int = name.hashCode() * 31 + type.hashCode()

    override fun toString(): String = "$name: $type"
}

/**
 * A value class, such as `@JvmInline value class UserId(val value: Long)`. The schema keeps the wrapper's identity
 * beside its underlying property, so `Long`, `UserId` and `OrderId` stay three schemas whatever their bytes are.
 */
public class ValueClassSchema(
    override val typeName: String,
    public val underlying: FieldSchema,
    typeParameters: List<String> = emptyList(),
) : DeclarationSchema() {
    override val typeParameters: List<String> = typeParameters.toList()

    init {
        validate(typeName, this.typeParameters)
        requireDeclared(typeName, this.typeParameters, underlying.type)
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is ValueClassSchema &&
            typeName == other.typeName &&
            typeParameters == other.typeParameters &&
            underlying == other.underlying

    override fun hashCode(): Int = (typeName.hashCode() * 31 + typeParameters.hashCode()) * 31 + underlying.hashCode()

    override fun toString(): String = "value class ${declaration(typeName, typeParameters)}($underlying)"
}

/** An enum class. Each constant has its persisted `@FlinkId`; a constant's name and position aren't its identity. */
public class EnumSchema(
    override val typeName: String,
    /** The constants, in declaration order. */
    constants: List<EnumConstantSchema>,
) : DeclarationSchema() {
    public val constants: List<EnumConstantSchema> = constants.toList()
    override val typeParameters: List<String> get() = emptyList()

    init {
        validate(typeName, emptyList())
        requireUnique(this.constants.map { it.name }) { "$typeName has two constants named $it" }
        requireUnique(this.constants.map { it.id }) { "$typeName has two constants with ID $it" }
    }

    override fun equals(other: Any?): Boolean =
        this === other || other is EnumSchema && typeName == other.typeName && constants == other.constants

    override fun hashCode(): Int = typeName.hashCode() * 31 + constants.hashCode()

    override fun toString(): String = "enum $typeName { ${constants.joinToString()} }"
}

/** One enum constant: its name and its `@FlinkId`, a positive `Int`. */
public class EnumConstantSchema(
    public val name: String,
    public val id: Int,
) {
    init {
        require(name.isNotEmpty()) { "An enum constant needs a name" }
        require(id > 0) { "The ID of $name is $id, but IDs are positive" }
    }

    override fun equals(other: Any?): Boolean =
        this === other || other is EnumConstantSchema && name == other.name && id == other.id

    override fun hashCode(): Int = name.hashCode() * 31 + id

    override fun toString(): String = "$name = $id"
}

/**
 * A sealed class or interface. Each direct subtype has its persisted `@FlinkId` in this type's own ID namespace, and
 * a schema of its own: a record, an object, or a nested sealed type with its own namespace.
 */
public class SealedSchema(
    override val typeName: String,
    /** The direct subtypes, in declaration order. */
    subtypes: List<SealedSubtypeSchema>,
    typeParameters: List<String> = emptyList(),
) : DeclarationSchema() {
    public val subtypes: List<SealedSubtypeSchema> = subtypes.toList()
    override val typeParameters: List<String> = typeParameters.toList()

    init {
        validate(typeName, this.typeParameters)
        requireUnique(this.subtypes.map { it.type.name }) { "$typeName lists the subtype $it twice" }
        requireUnique(this.subtypes.map { it.id }) { "$typeName has two subtypes with ID $it" }
        this.subtypes.forEach { requireDeclared(typeName, this.typeParameters, it.type) }
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is SealedSchema &&
            typeName == other.typeName &&
            typeParameters == other.typeParameters &&
            subtypes == other.subtypes

    override fun hashCode(): Int = (typeName.hashCode() * 31 + typeParameters.hashCode()) * 31 + subtypes.hashCode()

    override fun toString(): String = "sealed ${declaration(typeName, typeParameters)} { ${subtypes.joinToString()} }"
}

/**
 * A direct subtype of a sealed type: its `@FlinkId`, a positive `Int`, and its type. For a generic sealed type, the
 * subtype's arguments refer to the sealed type's type parameters.
 */
public class SealedSubtypeSchema(
    public val id: Int,
    public val type: SchemaType.ClassType,
) {
    init {
        require(id > 0) { "The ID of ${type.name} is $id, but IDs are positive" }
        require(!type.isNullable) { "A sealed subtype isn't nullable: ${type.name}" }
    }

    override fun equals(other: Any?): Boolean =
        this === other || other is SealedSubtypeSchema && id == other.id && type == other.type

    override fun hashCode(): Int = id * 31 + type.hashCode()

    override fun toString(): String = "$id = $type"
}

/** An `object` or `data object`. It's written as nothing but its identity, so it has no fields. */
public class ObjectSchema(
    override val typeName: String,
) : DeclarationSchema() {
    override val typeParameters: List<String> get() = emptyList()

    init {
        validate(typeName, emptyList())
    }

    override fun equals(other: Any?): Boolean = this === other || other is ObjectSchema && typeName == other.typeName

    override fun hashCode(): Int = typeName.hashCode()

    override fun toString(): String = "object $typeName"
}

private fun validate(typeName: String, typeParameters: List<String>) {
    require(typeName.isNotEmpty()) { "A declaration needs a type name" }
    require(typeParameters.none { it.isEmpty() }) { "A type parameter of $typeName has no name" }
    requireUnique(typeParameters) { "$typeName has two type parameters named $it" }
}

private inline fun <T> requireUnique(values: List<T>, message: (T) -> String) {
    val seen = HashSet<T>()
    for (value in values) require(seen.add(value)) { message(value) }
}

/** Every type parameter that [type] refers to is one of [typeParameters]. */
private fun requireDeclared(typeName: String, typeParameters: List<String>, type: SchemaType?) {
    when (type) {
        null -> Unit
        is SchemaType.TypeParameter ->
            require(type.name in typeParameters) { "$typeName refers to a type parameter ${type.name} it doesn't declare" }
        is SchemaType.ClassType -> type.arguments.forEach { requireDeclared(typeName, typeParameters, it.type) }
    }
}

private fun declaration(typeName: String, typeParameters: List<String>): String =
    if (typeParameters.isEmpty()) typeName else typeParameters.joinToString(", ", "$typeName<", ">")
