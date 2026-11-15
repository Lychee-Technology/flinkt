package io.flinkt.ksp.prototype

import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.Modifier

/**
 * A prototype of what KSP checks and computes for `@PersistedFieldOrder` (docs/persisted-format.md, "Field order"),
 * evidence for #5. #11 builds the real checks into the codec generator.
 *
 * For each `@FlinkType` record it computes the persisted order: the primary constructor's properties in constructor
 * order, or, when they carry `@PersistedFieldOrder`, in ascending order of their keys. It generates
 * `<Record>PersistedOrder.FIELDS`, the property names in that order, which the tests read. It reports an error,
 * located at the declaration, for an annotation on some of a record's properties and not all, for a negative or a
 * repeated key, and for an annotation on a property that isn't persisted.
 */
public class PersistedFieldOrderProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor =
        PersistedFieldOrderProcessor(environment.codeGenerator, environment.logger)
}

private const val ORDER = "io.flinkt.PersistedFieldOrder"
private const val FLINK_TYPE = "io.flinkt.FlinkType"
private const val RULE = "@PersistedFieldOrder orders the primary-constructor properties of a @FlinkType record"

private class PersistedFieldOrderProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
) : SymbolProcessor {
    override fun process(resolver: Resolver): List<KSAnnotated> {
        for (symbol in resolver.getSymbolsWithAnnotation(ORDER)) checkTarget(symbol)
        resolver.getSymbolsWithAnnotation(FLINK_TYPE)
            .filterIsInstance<KSClassDeclaration>()
            .filter(::isRecord)
            .forEach(::order)
        return emptyList()
    }

    /** An annotated declaration is a primary-constructor property of a `@FlinkType` record, the only kind persisted. */
    private fun checkTarget(symbol: KSAnnotated) {
        val property = symbol as? KSPropertyDeclaration
        val owner = property?.parentDeclaration as? KSClassDeclaration
        val name = property?.let { (owner?.qualifiedName?.asString()?.let { "$it." } ?: "") + it.simpleName.asString() }
        val problem = when {
            property == null || owner == null -> "${name ?: "a declaration"}, which isn't a property of a class"
            !owner.hasAnnotation(FLINK_TYPE) -> "$name, and ${owner.qualifiedName?.asString()} isn't a @FlinkType"
            Modifier.VALUE in owner.modifiers || Modifier.INLINE in owner.modifiers ->
                "$name, the property of a value class, which is persisted as its underlying value and has no field order"
            !isRecord(owner) -> "$name, and ${owner.qualifiedName?.asString()} isn't a record, whose properties are its fields"
            constructorProperties(owner).none { it.simpleName.asString() == property.simpleName.asString() } ->
                "$name, which isn't a primary-constructor property, so it isn't persisted"
            else -> return
        }
        logger.error("@PersistedFieldOrder is on $problem. $RULE", symbol)
    }

    /** Checks [record]'s keys, all or none, non-negative and unique, and generates its persisted order. */
    private fun order(record: KSClassDeclaration) {
        val recordName = record.qualifiedName?.asString() ?: record.simpleName.asString()
        val properties = constructorProperties(record)
        val keys = properties.associateWith(::orderKey)
        val annotated = properties.filter { keys[it] != null }
        val persisted = if (annotated.isEmpty()) {
            properties
        } else {
            if (annotated.size != properties.size) {
                val missing = properties.filter { keys[it] == null }
                logger.error(
                    "$recordName has @PersistedFieldOrder on ${names(annotated)} and not on ${names(missing)}: put it " +
                        "on every primary-constructor property, or on none",
                    record,
                )
                return
            }
            var valid = true
            val seen = HashMap<Int, KSPropertyDeclaration>()
            for (property in properties) {
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
            if (!valid) return
            properties.sortedBy { keys.getValue(it) }
        }
        val pkg = record.packageName.asString()
        val objectName = "${record.simpleName.asString()}PersistedOrder"
        codeGenerator.createNewFile(Dependencies(false, record.containingFile!!), pkg, objectName).bufferedWriter().use {
            it.write(
                (if (pkg.isEmpty()) "" else "package $pkg\n\n") +
                    "public object $objectName {\n" +
                    "    public val FIELDS: List<String> = listOf(${persisted.joinToString { p -> "\"${p.simpleName.asString()}\"" }})\n" +
                    "}\n",
            )
        }
    }

    private fun isRecord(declaration: KSClassDeclaration): Boolean =
        declaration.classKind == ClassKind.CLASS &&
            declaration.hasAnnotation(FLINK_TYPE) &&
            declaration.modifiers.none { it == Modifier.SEALED || it == Modifier.ABSTRACT || it == Modifier.VALUE || it == Modifier.INLINE }

    /** The properties the primary constructor declares, in constructor order: a record's persisted fields. */
    private fun constructorProperties(declaration: KSClassDeclaration): List<KSPropertyDeclaration> {
        val parameters = declaration.primaryConstructor?.parameters.orEmpty().filter { it.isVal || it.isVar }.map { it.name?.asString() }
        val properties = declaration.getDeclaredProperties().associateBy { it.simpleName.asString() }
        return parameters.mapNotNull { properties[it] }
    }

    private fun orderKey(property: KSPropertyDeclaration): Int? =
        property.annotations.firstOrNull { it.annotationType.resolve().declaration.qualifiedName?.asString() == ORDER }
            ?.arguments?.single { it.name?.asString() == "order" }?.value as Int?

    private fun KSClassDeclaration.hasAnnotation(name: String): Boolean =
        annotations.any { it.annotationType.resolve().declaration.qualifiedName?.asString() == name }

    private fun names(properties: List<KSPropertyDeclaration>): String = properties.joinToString { it.simpleName.asString() }
}
