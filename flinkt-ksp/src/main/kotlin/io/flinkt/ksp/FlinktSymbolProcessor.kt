package io.flinkt.ksp

import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.getVisibility
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFile
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.symbol.Visibility

/**
 * KSP processor for `@FlinkType` declarations (first slice: non-generic data classes whose fields are non-null
 * built-in scalars or other `@FlinkType` data classes in the same compilation).
 *
 * Output incremental classification (recorded in `docs/architecture.md`):
 * - Per-type codec file (`<MangledName>_FlinktCodec.kt`): **isolating**, with only that declaration's own containing
 *   file as its root source. Referenced declarations (such as nested `@FlinkType` field types and superclasses) are
 *   reached strictly through KSP symbol/type resolution so KSP tracks those edges via resolution tracing.
 * - Module registry (`FlinktGeneratedTypeModule_<hash>.kt`) and `META-INF/services/io.flinkt.registry.GeneratedTypeModule`:
 *   **aggregating**, with every `@FlinkType` declaration's containing file as root sources.
 */
internal class FlinktSymbolProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
    private val kotlinVersion: KotlinVersion,
) : SymbolProcessor {
    private var done = false

    override fun process(resolver: Resolver): List<KSAnnotated> {
        if (done) return emptyList()
        done = true

        checkMinimumKotlinVersion()

        var hasErrors = false
        for (symbol in resolver.getSymbolsWithAnnotation(PERSISTED_FIELD_ORDER)) {
            if (!validatePersistedFieldOrderTarget(symbol)) hasErrors = true
        }

        val annotated = resolver.getSymbolsWithAnnotation(FLINK_TYPE).toList()
        if (annotated.isEmpty()) return emptyList()

        val validRecords = mutableListOf<ValidatedRecord>()
        for (symbol in annotated) {
            val record = validateRecord(symbol)
            if (record == null) {
                hasErrors = true
            } else {
                validRecords += record
            }
        }

        if (hasErrors) return emptyList()

        // Sort deterministically by JVM binary name so generated registry output is reproducible across builds.
        val sortedRecords = validRecords.sortedBy { it.binaryName }
        for (record in sortedRecords) {
            emitRecordCodec(record)
        }
        emitModuleAndService(sortedRecords)
        return emptyList()
    }

    private fun checkMinimumKotlinVersion() {
        if (kotlinVersion < MINIMUM_KOTLIN_VERSION) {
            logger.warn(
                "flinkt-ksp is running with Kotlin language version $kotlinVersion, below the minimum supported " +
                    "version 2.4 (docs/kotlin-compatibility.md). Kotlin 2.4.0 or later is required.",
            )
        }
    }

    /**
     * Checks that every `@PersistedFieldOrder` sits on a primary-constructor property of a `@FlinkType` record.
     * Returns `true` if valid, `false` if an error was logged.
     */
    private fun validatePersistedFieldOrderTarget(symbol: KSAnnotated): Boolean {
        val property = symbol as? KSPropertyDeclaration
        val owner = property?.parentDeclaration as? KSClassDeclaration
        val name = property?.let { (owner?.qualifiedName?.asString()?.let { "$it." } ?: "") + it.simpleName.asString() }
        val problem = when {
            property == null || owner == null -> "${name ?: "a declaration"}, which isn't a property of a class"
            !owner.hasAnnotation(FLINK_TYPE) -> "$name, and ${owner.qualifiedName?.asString()} isn't a @FlinkType"
            Modifier.VALUE in owner.modifiers || Modifier.INLINE in owner.modifiers ->
                "$name, the property of a value class, which is persisted as its underlying value and has no field order"
            owner.classKind != ClassKind.CLASS || Modifier.DATA !in owner.modifiers ->
                "$name, and ${owner.qualifiedName?.asString()} isn't a record, whose properties are its fields"
            constructorProperties(owner).none { it.simpleName.asString() == property.simpleName.asString() } ->
                "$name, which isn't a primary-constructor property, so it isn't persisted"
            else -> return true
        }
        logger.error("@PersistedFieldOrder is on $problem. $ORDER_RULE", symbol)
        return false
    }

    private fun validateRecord(symbol: KSAnnotated): ValidatedRecord? {
        val declaration = symbol as? KSClassDeclaration
        if (declaration == null) {
            logger.error("@FlinkType applies to classes only", symbol)
            return null
        }
        val qualifiedName = declaration.qualifiedName?.asString()
        if (qualifiedName == null) {
            logger.error("@FlinkType can't be applied to local class ${declaration.simpleName.asString()}", declaration)
            return null
        }

        // Declaration kind checks (first slice supports non-generic data classes only).
        val unsupportedKind = when {
            Modifier.VALUE in declaration.modifiers || Modifier.INLINE in declaration.modifiers ->
                "$qualifiedName is a value class, and value classes aren't supported yet (#19)"
            declaration.classKind == ClassKind.ENUM_CLASS ->
                "$qualifiedName is an enum class, and enums aren't supported yet (#21)"
            Modifier.SEALED in declaration.modifiers ->
                "$qualifiedName is a sealed type, and sealed hierarchies aren't supported yet (#21)"
            declaration.classKind == ClassKind.OBJECT ->
                "$qualifiedName is an object, and objects aren't supported yet (#21)"
            declaration.classKind == ClassKind.INTERFACE ->
                "$qualifiedName is an interface; @FlinkType currently supports data classes only"
            declaration.classKind == ClassKind.ANNOTATION_CLASS ->
                "$qualifiedName is an annotation class; @FlinkType currently supports data classes only"
            declaration.classKind != ClassKind.CLASS || Modifier.DATA !in declaration.modifiers ->
                "$qualifiedName isn't a data class; @FlinkType currently supports data classes only"
            else -> null
        }
        if (unsupportedKind != null) {
            logger.error(unsupportedKind, declaration)
            return null
        }

        var valid = true

        if (declaration.typeParameters.isNotEmpty()) {
            val params = declaration.typeParameters.joinToString { it.name.asString() }
            logger.error("$qualifiedName<$params> is generic, and generic classes aren't supported yet (#22)", declaration)
            valid = false
        }

        if (Modifier.INNER in declaration.modifiers) {
            logger.error("$qualifiedName is an inner class and captures an outer instance, which can't be persisted", declaration)
            valid = false
        }

        // Visibility of class and every enclosing class.
        var enclosing: KSClassDeclaration? = declaration
        while (enclosing != null) {
            val vis = enclosing.getVisibility()
            if (vis == Visibility.PRIVATE || vis == Visibility.PROTECTED) {
                val target = if (enclosing == declaration) qualifiedName else "$qualifiedName's enclosing class ${enclosing.qualifiedName?.asString()}"
                logger.error(
                    "$target is ${vis.name.lowercase()}, so generated code can't access it. Make it public or internal",
                    enclosing,
                )
                valid = false
            }
            enclosing = enclosing.parentDeclaration as? KSClassDeclaration
        }

        val primaryConstructor = declaration.primaryConstructor
        if (primaryConstructor == null) {
            logger.error("$qualifiedName has no primary constructor", declaration)
            return null
        }
        val ctorVis = primaryConstructor.getVisibility()
        if (ctorVis == Visibility.PRIVATE || ctorVis == Visibility.PROTECTED) {
            logger.error(
                "$qualifiedName has a ${ctorVis.name.lowercase()} primary constructor, so generated code can't call it. " +
                    "Make it public or internal",
                primaryConstructor,
            )
            valid = false
        }

        val ctorProps = constructorProperties(declaration)
        val ctorPropNames = ctorProps.mapTo(HashSet()) { it.simpleName.asString() }

        // Check body properties: reject backing fields, lateinit, and delegated properties; allow computed getters.
        for (property in declaration.getDeclaredProperties()) {
            val propName = property.simpleName.asString()
            if (propName in ctorPropNames) continue
            when {
                property.isDelegated() -> {
                    logger.error(
                        "$qualifiedName.$propName is a delegated property, so its state wouldn't survive a restore. " +
                            "Use a computed getter or a primary-constructor property",
                        property,
                    )
                    valid = false
                }
                property.hasBackingField || Modifier.LATEINIT in property.modifiers -> {
                    logger.error(
                        "$qualifiedName.$propName is a class-body property with a backing field, so its value wouldn't " +
                            "survive a restore. Use a computed getter or a primary-constructor property",
                        property,
                    )
                    valid = false
                }
            }
        }

        // Check superclasses for inherited stored state.
        var superClass = declaration.superclass()
        while (superClass != null) {
            val superName = superClass.qualifiedName?.asString() ?: superClass.simpleName.asString()
            for (prop in superClass.getDeclaredProperties()) {
                if (Modifier.JAVA_STATIC in prop.modifiers) continue
                if (prop.isDelegated() || prop.hasBackingField || Modifier.LATEINIT in prop.modifiers) {
                    logger.error(
                        "$qualifiedName inherits stored state $superName.${prop.simpleName.asString()} from $superName, " +
                            "which wouldn't survive a restore",
                        declaration,
                    )
                    valid = false
                }
            }
            superClass = superClass.superclass()
        }

        // Validate primary-constructor property visibility and field types.
        val fields = mutableListOf<ValidatedField>()
        for (property in ctorProps) {
            val propName = property.simpleName.asString()
            val propVis = property.getVisibility()
            val getterModifiers = property.getter?.modifiers.orEmpty()
            val restrictedVis = when {
                propVis == Visibility.PRIVATE || Modifier.PRIVATE in getterModifiers -> Visibility.PRIVATE
                propVis == Visibility.PROTECTED || Modifier.PROTECTED in getterModifiers -> Visibility.PROTECTED
                else -> null
            }
            if (restrictedVis != null) {
                logger.error(
                    "$qualifiedName.$propName has a ${restrictedVis.name.lowercase()} getter, so generated code can't " +
                        "read it. Make it public or internal",
                    property,
                )
                valid = false
            }

            val fieldKind = validateFieldType(qualifiedName, property)
            if (fieldKind == null) {
                valid = false
            } else {
                fields += ValidatedField(
                    property = property,
                    name = propName,
                    isVar = property.isMutable,
                    kind = fieldKind,
                )
            }
        }

        // Validate @PersistedFieldOrder keys (all-or-none, non-negative, unique).
        val persistedFields = validatePersistedOrder(declaration, qualifiedName, ctorProps, fields)
        if (persistedFields == null) {
            valid = false
        }

        if (!valid) return null

        // Check for recursive cycles across nested @FlinkType fields.
        if (!checkAcyclic(declaration, qualifiedName, fields)) {
            return null
        }

        val containingFile = declaration.containingFile
        if (containingFile == null) {
            logger.error("$qualifiedName has no source file in this compilation", declaration)
            return null
        }

        return ValidatedRecord(
            declaration = declaration,
            containingFile = containingFile,
            packageName = declaration.packageName.asString(),
            binaryName = declaration.binaryName(),
            kotlinReference = declaration.kotlinReference(),
            codecSimpleName = declaration.codecSimpleName(),
            constructorFields = fields,
            persistedFields = persistedFields!!,
        )
    }

    private fun validateFieldType(ownerName: String, property: KSPropertyDeclaration): FieldKind? {
        val propName = property.simpleName.asString()
        val resolved = property.type.resolve().unwrapTypeAliases()
        val typeDesc = describeType(resolved)

        if (resolved.isError) {
            logger.error("$ownerName.$propName: $typeDesc couldn't be resolved", property)
            return null
        }
        if (resolved.isMarkedNullable) {
            logger.error("$ownerName.$propName: $typeDesc is nullable, and nullable fields aren't supported yet (#15)", property)
            return null
        }

        val fieldDecl = resolved.declaration as? KSClassDeclaration
        if (fieldDecl == null) {
            logger.error("$ownerName.$propName: $typeDesc isn't a supported field type", property)
            return null
        }

        val fieldQualifiedName = fieldDecl.qualifiedName?.asString()
        if (fieldQualifiedName != null) {
            SCALARS[fieldQualifiedName]?.let { return FieldKind.Scalar(it) }
            if (fieldQualifiedName in SUPPORTED_COLLECTION_NAMES) {
                logger.error(
                    "$ownerName.$propName: $typeDesc isn't supported yet; List and Map collections are #17",
                    property,
                )
                return null
            }
            if (fieldQualifiedName in UNSUPPORTED_COLLECTION_NAMES) {
                logger.error(
                    "$ownerName.$propName: $typeDesc is an unsupported collection or array type",
                    property,
                )
                return null
            }
        }

        when {
            Modifier.VALUE in fieldDecl.modifiers || Modifier.INLINE in fieldDecl.modifiers -> {
                logger.error("$ownerName.$propName: $typeDesc is a value class, and value classes aren't supported yet (#19)", property)
                return null
            }
            fieldDecl.classKind == ClassKind.ENUM_CLASS -> {
                logger.error("$ownerName.$propName: $typeDesc is an enum, and enums aren't supported yet (#21)", property)
                return null
            }
            Modifier.SEALED in fieldDecl.modifiers -> {
                logger.error("$ownerName.$propName: $typeDesc is a sealed type, and sealed hierarchies aren't supported yet (#21)", property)
                return null
            }
            fieldDecl.classKind == ClassKind.OBJECT -> {
                logger.error("$ownerName.$propName: $typeDesc is an object, and objects aren't supported yet (#21)", property)
                return null
            }
            !fieldDecl.hasAnnotation(FLINK_TYPE) -> {
                logger.error(
                    "$ownerName.$propName: $typeDesc isn't a built-in type or a @FlinkType data class",
                    property,
                )
                return null
            }
            fieldDecl.typeParameters.isNotEmpty() || resolved.arguments.isNotEmpty() -> {
                logger.error("$ownerName.$propName: $typeDesc is generic, and generic classes aren't supported yet (#22)", property)
                return null
            }
            fieldDecl.classKind != ClassKind.CLASS || Modifier.DATA !in fieldDecl.modifiers -> {
                logger.error("$ownerName.$propName: $typeDesc isn't a data class", property)
                return null
            }
            fieldDecl.containingFile == null -> {
                logger.error(
                    "$ownerName.$propName: $typeDesc is declared in another module, and cross-module @FlinkType " +
                        "references aren't supported yet (#24)",
                    property,
                )
                return null
            }
            else -> return FieldKind.NestedRecord(
                declaration = fieldDecl,
                binaryName = fieldDecl.binaryName(),
                kotlinReference = fieldDecl.kotlinReference(),
            )
        }
    }

    private fun validatePersistedOrder(
        record: KSClassDeclaration,
        recordName: String,
        ctorProps: List<KSPropertyDeclaration>,
        fields: List<ValidatedField>,
    ): List<ValidatedField>? {
        val keys = ctorProps.associateWith(::orderKey)
        val annotated = ctorProps.filter { keys[it] != null }
        if (annotated.isEmpty()) return fields

        if (annotated.size != ctorProps.size) {
            val missing = ctorProps.filter { keys[it] == null }
            logger.error(
                "$recordName has @PersistedFieldOrder on ${names(annotated)} and not on ${names(missing)}: put it " +
                    "on every primary-constructor property, or on none",
                record,
            )
            return null
        }

        var valid = true
        val seen = HashMap<Int, KSPropertyDeclaration>()
        for (property in ctorProps) {
            val key = keys.getValue(property)!!
            val name = "$recordName.${property.simpleName.asString()}"
            if (key < 0) {
                logger.error("$name has @PersistedFieldOrder(order = $key): an order is a non-negative Int", property)
                valid = false
            }
            val other = seen.putIfAbsent(key, property)
            if (other != null) {
                logger.error(
                    "$name has @PersistedFieldOrder(order = $key), as $recordName.${other.simpleName.asString()} " +
                        "has: each property's order is unique",
                    property,
                )
                valid = false
            }
        }
        if (!valid) return null

        val fieldByProp = fields.associateBy { it.property }
        return ctorProps.sortedBy { keys.getValue(it) }.mapNotNull { fieldByProp[it] }
    }

    /**
     * Checks that [record] does not reach itself through nested `@FlinkType` data-class fields.
     * Walks only via KSP type resolution and caps depth to guard against malformed cyclic graphs.
     */
    private fun checkAcyclic(
        record: KSClassDeclaration,
        recordName: String,
        fields: List<ValidatedField>,
    ): Boolean {
        val rootBinaryName = record.binaryName()
        var valid = true
        for (field in fields) {
            val nested = field.kind as? FieldKind.NestedRecord ?: continue
            val cycle = findCycle(
                current = nested.declaration,
                targetBinaryName = rootBinaryName,
                path = mutableListOf(recordName),
                visited = HashSet(),
            )
            if (cycle != null) {
                logger.error(
                    "$recordName.${field.name}: ${nested.declaration.qualifiedName?.asString()} forms a recursive " +
                        "cycle (${cycle.joinToString(" -> ")}), and recursive types aren't supported yet",
                    field.property,
                )
                valid = false
            }
        }
        return valid
    }

    private fun findCycle(
        current: KSClassDeclaration,
        targetBinaryName: String,
        path: MutableList<String>,
        visited: MutableSet<String>,
    ): List<String>? {
        val currentBinary = current.binaryName()
        val currentName = current.qualifiedName?.asString() ?: currentBinary
        if (currentBinary == targetBinaryName) {
            return path + currentName
        }
        if (path.size >= MAX_CYCLE_DEPTH || !visited.add(currentBinary)) return null
        path.add(currentName)
        for (prop in constructorProperties(current)) {
            val resolved = prop.type.resolve().unwrapTypeAliases()
            if (resolved.isError || resolved.isMarkedNullable) continue
            val next = resolved.declaration as? KSClassDeclaration ?: continue
            if (next.classKind != ClassKind.CLASS || Modifier.DATA !in next.modifiers) continue
            if (!next.hasAnnotation(FLINK_TYPE) || next.containingFile == null) continue
            val cycle = findCycle(next, targetBinaryName, path, visited)
            if (cycle != null) return cycle
        }
        path.removeAt(path.lastIndex)
        return null
    }

    private fun emitRecordCodec(record: ValidatedRecord) {
        val nestedTypes = record.persistedFields
            .map { it.kind }
            .filterIsInstance<FieldKind.NestedRecord>()
            .distinctBy { it.binaryName }
        val nestedCodecSlot = nestedTypes.withIndex().associate { (i, nested) -> nested.binaryName to "_c$i" }
        val hasNested = nestedTypes.isNotEmpty()
        val isDeeplyImmutable = !hasNested && record.constructorFields.none { it.isVar }

        val source = buildString {
            append("// Generated by flinkt-ksp for ").append(record.binaryName).append(". Don't edit it.\n")
            if (record.packageName.isNotEmpty()) {
                append("package ").append(qualified(record.packageName)).append("\n\n")
            }
            if (hasNested) {
                append("import io.flinkt.codec.codec as _flinktCodec\n\n")
            }

            val ref = record.kotlinReference
            if (!hasNested) {
                append("internal object ").append(record.codecSimpleName)
                    .append(" : io.flinkt.codec.GeneratedCodec<").append(ref).append(">, ")
                    .append("io.flinkt.codec.CodecFactory<").append(ref).append("> {\n")
                appendSchema(record, indent = "    ")
                append("\n")
                append("    override fun create(context: io.flinkt.codec.CodecContext): io.flinkt.codec.GeneratedCodec<")
                    .append(ref).append("> = this\n")
            } else {
                append("internal class ").append(record.codecSimpleName).append("(\n")
                for ((index, nested) in nestedTypes.withIndex()) {
                    append("    private val _c").append(index)
                        .append(": io.flinkt.codec.GeneratedCodec<").append(nested.kotlinReference).append(">,\n")
                }
                append(") : io.flinkt.codec.GeneratedCodec<").append(ref).append("> {\n")
            }

            // serialize
            append("\n")
            append("    override fun serialize(value: ").append(ref).append(", output: java.io.DataOutput) {\n")
            for (field in record.persistedFields) {
                val access = "value.${identifier(field.name)}"
                when (val kind = field.kind) {
                    is FieldKind.Scalar -> append("        io.flinkt.codec.RecordFormat.")
                        .append(kind.scalar.writeMethod).append("(output, ").append(access).append(")\n")
                    is FieldKind.NestedRecord -> append("        ")
                        .append(nestedCodecSlot.getValue(kind.binaryName))
                        .append(".serialize(").append(access).append(", output)\n")
                }
            }
            append("    }\n")

            // deserialize
            append("\n")
            append("    override fun deserialize(input: java.io.DataInput): ").append(ref).append(" {\n")
            val localForField = HashMap<String, String>(record.persistedFields.size)
            for ((index, field) in record.persistedFields.withIndex()) {
                val local = "_f$index"
                localForField[field.name] = local
                when (val kind = field.kind) {
                    is FieldKind.Scalar -> append("        val ").append(local)
                        .append(" = io.flinkt.codec.RecordFormat.").append(kind.scalar.readMethod).append("(input)\n")
                    is FieldKind.NestedRecord -> append("        val ").append(local)
                        .append(" = ").append(nestedCodecSlot.getValue(kind.binaryName)).append(".deserialize(input)\n")
                }
            }
            append("        return ").append(ref).append("(\n")
            for (field in record.constructorFields) {
                append("            ").append(identifier(field.name)).append(" = ")
                    .append(localForField.getValue(field.name)).append(",\n")
            }
            append("        )\n")
            append("    }\n")

            // copy
            append("\n")
            if (isDeeplyImmutable) {
                append("    override fun copy(value: ").append(ref).append("): ").append(ref).append(" = value\n")
            } else {
                append("    override fun copy(value: ").append(ref).append("): ").append(ref).append(" = ")
                    .append(ref).append("(\n")
                for (field in record.constructorFields) {
                    val access = "value.${identifier(field.name)}"
                    val copied = when (val kind = field.kind) {
                        is FieldKind.Scalar -> access
                        is FieldKind.NestedRecord -> "${nestedCodecSlot.getValue(kind.binaryName)}.copy($access)"
                    }
                    append("        ").append(identifier(field.name)).append(" = ").append(copied).append(",\n")
                }
                append("    )\n")
            }

            if (hasNested) {
                append("\n")
                append("    internal object Factory : io.flinkt.codec.CodecFactory<").append(ref).append("> {\n")
                appendSchema(record, indent = "        ")
                append("\n")
                append("        override fun create(context: io.flinkt.codec.CodecContext): io.flinkt.codec.GeneratedCodec<")
                    .append(ref).append("> =\n")
                append("            ").append(record.codecSimpleName).append("(\n")
                for (nested in nestedTypes) {
                    append("                context._flinktCodec<").append(nested.kotlinReference).append(">(),\n")
                }
                append("            )\n")
                append("    }\n")
            }

            append("}\n")
        }

        // Isolating output: only this record's containing file is a root source; referenced types are tracked by
        // KSP's resolution tracing.
        codeGenerator.createNewFile(
            Dependencies(aggregating = false, record.containingFile),
            record.packageName,
            record.codecSimpleName,
        ).bufferedWriter().use { it.write(source) }
    }

    private fun StringBuilder.appendSchema(record: ValidatedRecord, indent: String) {
        append(indent).append("internal val schema: io.flinkt.schema.RecordSchema = io.flinkt.schema.RecordSchema(\n")
        append(indent).append("    typeName = ").append(stringLiteral(record.binaryName)).append(",\n")
        append(indent).append("    fields = listOf(\n")
        for (field in record.constructorFields) {
            val schemaTypeName = when (val kind = field.kind) {
                is FieldKind.Scalar -> kind.scalar.kotlinName
                is FieldKind.NestedRecord -> kind.binaryName
            }
            append(indent).append("        io.flinkt.schema.FieldSchema(")
                .append(stringLiteral(field.name))
                .append(", io.flinkt.schema.SchemaType.ClassType(")
                .append(stringLiteral(schemaTypeName))
                .append(")),\n")
        }
        append(indent).append("    ),\n")
        append(indent).append("    persistedOrder = listOf(")
            .append(record.persistedFields.joinToString(", ") { stringLiteral(it.name) })
            .append("),\n")
        append(indent).append(")\n")
    }

    private fun emitModuleAndService(records: List<ValidatedRecord>) {
        // Code in a named package can't reference classes in the unnamed (root) package, so emit the module in the
        // unnamed package if any @FlinkType declaration is in it; otherwise use io.flinkt.generated.
        val hasRootPackageRecord = records.any { it.packageName.isEmpty() }
        val modulePackage = if (hasRootPackageRecord) "" else GENERATED_MODULE_PACKAGE
        val hash = sha256Hex16(records.map { it.binaryName })
        val moduleSimpleName = "FlinktGeneratedTypeModule_$hash"
        val moduleBinaryName = if (modulePackage.isEmpty()) moduleSimpleName else "$modulePackage.$moduleSimpleName"

        val origins = records.map { it.containingFile }.distinct().toTypedArray()
        val dependencies = Dependencies(aggregating = true, *origins)

        val moduleSource = buildString {
            append("// Generated by flinkt-ksp. Don't edit it.\n")
            if (modulePackage.isNotEmpty()) {
                append("package ").append(modulePackage).append("\n\n")
            }
            append("public class ").append(moduleSimpleName).append(" : io.flinkt.registry.GeneratedTypeModule {\n")
            append("    override fun register(registry: io.flinkt.registry.MutableTypeRegistry) {\n")
            for (record in records) {
                val codecRef = if (record.packageName.isEmpty()) {
                    record.codecSimpleName
                } else {
                    "${qualified(record.packageName)}.${record.codecSimpleName}"
                }
                val hasNested = record.persistedFields.any { it.kind is FieldKind.NestedRecord }
                val factoryRef = if (hasNested) "$codecRef.Factory" else codecRef
                append("        registry.register(\n")
                append("            ").append(record.kotlinReference).append("::class,\n")
                append("            ").append(factoryRef).append(".schema,\n")
                append("            ").append(factoryRef).append(",\n")
                append("        )\n")
            }
            append("    }\n")
            append("}\n")
        }

        codeGenerator.createNewFile(dependencies, modulePackage, moduleSimpleName)
            .bufferedWriter()
            .use { it.write(moduleSource) }

        codeGenerator.createNewFile(
            dependencies,
            "",
            "META-INF/services/io.flinkt.registry.GeneratedTypeModule",
            "",
        ).bufferedWriter().use {
            it.write(moduleBinaryName)
            it.write("\n")
        }
    }

    private fun constructorProperties(declaration: KSClassDeclaration): List<KSPropertyDeclaration> {
        val paramNames = declaration.primaryConstructor?.parameters.orEmpty()
            .filter { it.isVal || it.isVar }
            .mapNotNull { it.name?.asString() }
        val declared = declaration.getDeclaredProperties().associateBy { it.simpleName.asString() }
        return paramNames.mapNotNull { declared[it] }
    }

    private fun orderKey(property: KSPropertyDeclaration): Int? =
        property.annotations
            .firstOrNull { it.qualifiedName() == PERSISTED_FIELD_ORDER }
            ?.arguments
            ?.firstOrNull { it.name?.asString() == "order" }
            ?.value as? Int

    private fun names(properties: List<KSPropertyDeclaration>): String =
        properties.joinToString { it.simpleName.asString() }

    private class ValidatedRecord(
        val declaration: KSClassDeclaration,
        val containingFile: KSFile,
        val packageName: String,
        val binaryName: String,
        val kotlinReference: String,
        val codecSimpleName: String,
        val constructorFields: List<ValidatedField>,
        val persistedFields: List<ValidatedField>,
    )

    private class ValidatedField(
        val property: KSPropertyDeclaration,
        val name: String,
        val isVar: Boolean,
        val kind: FieldKind,
    )

    private sealed class FieldKind {
        class Scalar(val scalar: ScalarKind) : FieldKind()
        class NestedRecord(
            val declaration: KSClassDeclaration,
            val binaryName: String,
            val kotlinReference: String,
        ) : FieldKind()
    }

    private enum class ScalarKind(
        val kotlinName: String,
        val writeMethod: String,
        val readMethod: String,
    ) {
        BOOLEAN("kotlin.Boolean", "writeBoolean", "readBoolean"),
        BYTE("kotlin.Byte", "writeByte", "readByte"),
        SHORT("kotlin.Short", "writeShort", "readShort"),
        CHAR("kotlin.Char", "writeChar", "readChar"),
        INT("kotlin.Int", "writeInt", "readInt"),
        LONG("kotlin.Long", "writeLong", "readLong"),
        FLOAT("kotlin.Float", "writeFloat", "readFloat"),
        DOUBLE("kotlin.Double", "writeDouble", "readDouble"),
        STRING("kotlin.String", "writeString", "readString"),
    }

    private companion object {
        val MINIMUM_KOTLIN_VERSION: KotlinVersion = KotlinVersion(2, 4)

        const val FLINK_TYPE: String = "io.flinkt.FlinkType"
        const val PERSISTED_FIELD_ORDER: String = "io.flinkt.PersistedFieldOrder"
        const val GENERATED_MODULE_PACKAGE: String = "io.flinkt.generated"
        const val ORDER_RULE: String = "@PersistedFieldOrder orders the primary-constructor properties of a @FlinkType record"
        const val MAX_CYCLE_DEPTH: Int = 64

        val SCALARS: Map<String, ScalarKind> = ScalarKind.entries.associateBy { it.kotlinName }

        val SUPPORTED_COLLECTION_NAMES: Set<String> = setOf(
            "kotlin.collections.List",
            "kotlin.collections.Map",
        )

        val UNSUPPORTED_COLLECTION_NAMES: Set<String> = setOf(
            "kotlin.collections.Set",
            "kotlin.collections.Collection",
            "kotlin.collections.Iterable",
            "kotlin.collections.MutableList",
            "kotlin.collections.MutableMap",
            "kotlin.collections.MutableSet",
            "kotlin.collections.MutableCollection",
            "kotlin.collections.MutableIterable",
            "kotlin.Array",
            "kotlin.BooleanArray",
            "kotlin.ByteArray",
            "kotlin.ShortArray",
            "kotlin.CharArray",
            "kotlin.IntArray",
            "kotlin.LongArray",
            "kotlin.FloatArray",
            "kotlin.DoubleArray",
        )
    }
}
