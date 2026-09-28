// SPIKE (disposable): generates the forwarding members of Flinkt's views from the Flink classes on the
// compile classpath, and fails the build when a requested method would break a view rule.
package flinkt.codegen

import com.google.devtools.ksp.KspExperimental
import com.google.devtools.ksp.getDeclaredFunctions
import com.google.devtools.ksp.isPublic
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSType
import com.google.devtools.ksp.symbol.KSTypeParameter
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.Modifier
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.TypeVariableName
import com.squareup.kotlinpoet.ANY
import com.squareup.kotlinpoet.LIST
import com.squareup.kotlinpoet.MUTABLE_LIST
import com.squareup.kotlinpoet.MAP
import com.squareup.kotlinpoet.MUTABLE_MAP
import com.squareup.kotlinpoet.SET
import com.squareup.kotlinpoet.MUTABLE_SET
import com.squareup.kotlinpoet.COLLECTION
import com.squareup.kotlinpoet.MUTABLE_COLLECTION
import com.squareup.kotlinpoet.ParameterizedTypeName
import com.squareup.kotlinpoet.ksp.TypeParameterResolver
import com.squareup.kotlinpoet.ksp.toClassName
import com.squareup.kotlinpoet.ksp.toTypeName
import com.squareup.kotlinpoet.ksp.toTypeParameterResolver
import com.squareup.kotlinpoet.ksp.toTypeVariableName
import com.squareup.kotlinpoet.ksp.writeTo

class ViewCodegenProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor =
        ViewCodegen(environment.codeGenerator, environment.logger, environment.options)
}

private const val ANNOTATION = "flinkt.codegen.FlinkView"
private const val STREAM_PACKAGE = "org.apache.flink.streaming.api.datastream."
private const val SINK = "org.apache.flink.streaming.api.datastream.DataStreamSink"

private class ViewSpec(val view: KSClassDeclaration, val flink: KSClassDeclaration, val forward: List<String>) {
    val forwardersName = ClassName(view.packageName.asString(), view.simpleName.asString() + "Forwarders")
}

class ViewCodegen(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
    private val options: Map<String, String>,
) : SymbolProcessor {
    private var done = false
    private val report = StringBuilder()
    private lateinit var resolver: Resolver

    /** Parameter names read from Flink's class files, keyed by class#name+descriptor. */
    private class ApiEntry(val names: List<String>)
    private val api: Map<String, ApiEntry> by lazy {
        val path = options["flinkt.flinkApi"] ?: error("ksp arg flinkt.flinkApi (the extracted Flink API file) is missing")
        val lines = java.io.File(path).readLines()
        val fileVersion = lines.firstOrNull { it.startsWith("# flink-version\t") }?.substringAfter('\t')
        if (fileVersion != options["flinkt.flinkVersion"]) {
            logger.error("$path was extracted from Flink $fileVersion, but this adapter builds against Flink ${options["flinkt.flinkVersion"]}; regenerate it")
        }
        lines.filter { !it.startsWith("#") && it.isNotBlank() }.associate { line ->
            val (cls, name, descriptor, names) = (line.split('\t') + "").take(4)
            "$cls#$name$descriptor" to ApiEntry(if (names.isEmpty()) emptyList() else names.split(','))
        }
    }

    override fun process(resolver: Resolver): List<KSAnnotated> {
        if (done) return emptyList()
        this.resolver = resolver
        val annotated = resolver.getSymbolsWithAnnotation(ANNOTATION).filterIsInstance<KSClassDeclaration>().toList()
        if (annotated.isEmpty()) return emptyList()
        done = true
        // Test hook for the negative cases: flinkt.extraForward=<flink class FQN>#<method>,...
        val extra = options["flinkt.extraForward"].orEmpty().split(',').filter { it.isNotBlank() }
            .groupBy({ it.substringBefore('#') }, { it.substringAfter('#') })
        val specs = annotated.map { view ->
            val ann = view.annotations.single { it.shortName.asString() == "FlinkView" }
            val flinkType = ann.arguments.single { it.name?.asString() == "flinkClass" }.value as KSType
            val flink = flinkType.declaration as KSClassDeclaration
            val forward = (ann.arguments.single { it.name?.asString() == "forward" }.value as List<*>).map { it as String } +
                extra[flink.qualifiedName!!.asString()].orEmpty()
            ViewSpec(view, flink, forward)
        }
        val duplicates = specs.groupBy { it.flink.qualifiedName!!.asString() }.filterValues { it.size > 1 }
        duplicates.forEach { (flink, s) -> logger.error("$flink has more than one view: ${s.map { it.view.simpleName.asString() }}", s.first().view) }
        val viewOf = specs.associateBy { it.flink.qualifiedName!!.asString() }
        report.appendLine("Flink ${options["flinkt.flinkVersion"] ?: "?"}")
        specs.forEach { generate(it, viewOf) }
        codeGenerator.createNewFile(Dependencies(false), "flinkt.views", "view-codegen-report", "txt")
            .use { it.write(report.toString().toByteArray()) }
        return emptyList()
    }

    private fun generate(spec: ViewSpec, viewOf: Map<String, ViewSpec>) {
        val flink = spec.flink
        val flinkName = flink.qualifiedName!!.asString()
        val classTypeParams = flink.typeParameters.toTypeParameterResolver()
        val typeVars = flink.typeParameters.map { typeVar(it, classTypeParams) }
        val flinkType = flink.toClassName().parameterizedBy(typeVars)
        val parent = flink.superTypes.map { it.resolve() }
            .firstOrNull { it.declaration.qualifiedName?.asString() in viewOf }
        val iface = TypeSpec.interfaceBuilder(spec.forwardersName)
            .addModifiers(KModifier.PUBLIC, KModifier.SEALED)
            .addTypeVariables(typeVars)
        if (parent != null) {
            val parentSpec = viewOf.getValue(parent.declaration.qualifiedName!!.asString())
            iface.addSuperinterface(parentSpec.forwardersName.parameterizedBy(parent.arguments.map { it.toTypeName(classTypeParams) }))
        }
        iface.addFunction(
            FunSpec.builder("asFlink")
                .addModifiers(KModifier.PUBLIC, KModifier.ABSTRACT)
                .apply { if (parent != null) addModifiers(KModifier.OVERRIDE) }
                .returns(flinkType)
                .build(),
        )
        report.appendLine("\n== ${spec.view.simpleName.asString()} <- $flinkName")
        for (name in spec.forward) {
            val overloads = flink.getDeclaredFunctions().filter { it.simpleName.asString() == name }.toList()
            if (overloads.isEmpty()) {
                logger.error("$name is not declared on $flinkName in Flink ${options["flinkt.flinkVersion"]}; remove it from @FlinkView or move it to a line-specific view", spec.view)
                continue
            }
            for (fn in overloads) {
                forward(spec, fn, viewOf, classTypeParams)?.let(iface::addFunction)
            }
        }
        FileSpec.builder(spec.forwardersName)
            .addFileComment("Generated by the Flinkt view codegen. Do not edit.")
            .addAnnotation(AnnotationSpec.builder(Suppress::class).addMember("%S", "DEPRECATION").useSiteTarget(AnnotationSpec.UseSiteTarget.FILE).build())
            .addType(iface.build())
            .build()
            .writeTo(codeGenerator, aggregating = false, originatingKSFiles = listOfNotNull(spec.view.containingFile))
    }

    private fun forward(
        spec: ViewSpec,
        fn: KSFunctionDeclaration,
        viewOf: Map<String, ViewSpec>,
        classTypeParams: TypeParameterResolver,
    ): FunSpec? {
        val flinkName = spec.flink.qualifiedName!!.asString()
        val signature = "$flinkName.${fn.simpleName.asString()}(${fn.parameters.joinToString { describe(it) }})"
        val annotations = fn.annotations.map { it.shortName.asString() }.toSet()
        report.appendLine("  $signature -> ${fn.returnType?.resolve()} ${fn.modifiers.sorted()} @$annotations")
        if (!fn.isPublic() || Modifier.JAVA_STATIC in fn.modifiers) return null.also { report.appendLine("    skipped: not a public instance method") }
        // Parity with Flink: deprecated overloads are forwarded, and a deprecated Flink method stays deprecated in Kotlin.
        val deprecated = "Deprecated" in annotations
        val signatureTypes = fn.parameters.map { it.type.resolve() } + listOfNotNull(fn.returnType?.resolve())
        signatureTypes.firstOrNull { mentionsDeprecated(it) }?.let { t ->
            report.appendLine("    note: signature uses deprecated type ${t.declaration.qualifiedName?.asString()}; forwarded as Flink declares it")
        }
        if ("Internal" in annotations) return fail(spec, "$signature is @Internal; views call only public Flink methods")
        // Parity with Flink: an @Experimental method is forwarded, and stays explicitly experimental in Kotlin.
        val experimental = "Experimental" in annotations

        @OptIn(KspExperimental::class)
        val key = "$flinkName#${fn.simpleName.asString()}${varargsFixed(resolver.mapToJvmSignature(fn)!!, fn.parameters.lastOrNull()?.isVararg == true)}"
        val entry = api[key] ?: return fail(spec, "$signature has no entry $key in the extracted Flink API file; regenerate it for this Flink version")
        if (entry.names.size != fn.parameters.size) return fail(spec, "$key: the extracted file lists ${entry.names.size} parameter names, the class has ${fn.parameters.size}")
        currentNames = entry.names
        val methodTypeParams = fn.typeParameters.map { it.name.asString() }.toSet()
        val resolver = fn.typeParameters.toTypeParameterResolver(classTypeParams)
        val returnType = fn.returnType!!.resolve()
        val returnName = returnType.declaration.qualifiedName?.asString()
        val wrapInto = viewOf[returnName]
        val body = CodeBlock.builder()
        val call = CodeBlock.of("asFlink().%N(%L)", fn.simpleName.asString(), fn.parameters.mapIndexed { i, p -> argument(p, paramName(fn, p, i), viewOf) }.joinToCode())
        val kotlinReturn: TypeName = when {
            mentions(returnType, methodTypeParams) ->
                return fail(spec, "$signature introduces an element type ($returnType); write it by hand with a reified type parameter")
            wrapInto != null -> {
                body.add("return %T(%L)", wrapInto.view.toClassName(), call)
                wrapInto.view.toClassName().parameterizedBy(returnType.arguments.map { it.toTypeName(resolver) })
            }
            returnName != null && returnName.startsWith(STREAM_PACKAGE) && returnName != SINK ->
                return fail(spec, "$signature returns $returnName, which has no view; forwarding it would let a chain leave the view without asFlink()")
            else -> {
                body.add("return %L", call)
                readOnly(returnType.toTypeName(resolver))
            }
        }
        report.appendLine("    generated -> $kotlinReturn${if (deprecated) " (deprecated)" else ""}${if (experimental) " (experimental, opt-in)" else ""}")
        return FunSpec.builder(fn.simpleName.asString())
            .addModifiers(KModifier.PUBLIC)
            .addTypeVariables(fn.typeParameters.map { typeVar(it, resolver) })
            .addParameters(fn.parameters.mapIndexed { i, p -> parameter(fn, p, i, viewOf, resolver) })
            .returns(kotlinReturn)
            .apply { if (experimental) addAnnotation(ClassName(spec.view.packageName.asString(), "ExperimentalFlinkApi")) }
            .apply {
                if (deprecated) {
                    addAnnotation(
                        AnnotationSpec.builder(Deprecated::class)
                            .addMember("%S", "Deprecated in Apache Flink: ${spec.flink.simpleName.asString()}.${fn.simpleName.asString()}")
                            .build(),
                    )
                }
            }
            .addCode(body.build())
            .build()
    }

    private fun fail(spec: ViewSpec, message: String): FunSpec? {
        logger.error(message, spec.view)
        report.appendLine("    ERROR: $message")
        return null
    }

    /** The element type of a Java varargs parameter. */
    private fun elementType(p: KSValueParameter): KSType {
        val t = p.type.resolve()
        return if (p.isVararg && t.declaration.qualifiedName?.asString() == "kotlin.Array") t.arguments.single().type!!.resolve() else t
    }

    private fun parameter(fn: KSFunctionDeclaration, p: KSValueParameter, index: Int, viewOf: Map<String, ViewSpec>, resolver: TypeParameterResolver): ParameterSpec {
        val t = elementType(p)
        val view = viewOf[t.declaration.qualifiedName?.asString()]
        var type: TypeName = if (view != null) view.view.toClassName().parameterizedBy(t.arguments.map { it.toTypeName(resolver) }) else t.toTypeName(resolver)
        if (p.annotations.any { it.shortName.asString() == "Nullable" }) type = type.copy(nullable = true)
        return ParameterSpec.builder(paramName(fn, p, index), type).apply { if (p.isVararg) addModifiers(KModifier.VARARG) }.build()
    }

    private fun argument(p: KSValueParameter, name: String, viewOf: Map<String, ViewSpec>): CodeBlock {
        val view = viewOf[elementType(p).declaration.qualifiedName?.asString()]
        return when {
            view != null && p.isVararg -> CodeBlock.of("*%N.map { it.asFlink() }.toTypedArray()", name)
            view != null -> CodeBlock.of("%N.asFlink()", name)
            p.isVararg -> CodeBlock.of("*%N", name)
            else -> CodeBlock.of("%N", name)
        }
    }

    private var currentNames: List<String> = emptyList()

    /** Names come from the extracted Flink API file; Flink's jars carry none (they read as p0, p1, ...). */
    private fun paramName(fn: KSFunctionDeclaration, p: KSValueParameter, index: Int): String = currentNames[index]

    private fun describe(p: KSValueParameter) = (if (p.isVararg) "vararg " else "") + "${p.name?.asString()}: ${p.type.resolve()}"

    private fun mentionsDeprecated(t: KSType): Boolean =
        t.declaration.annotations.any { it.shortName.asString() == "Deprecated" } ||
            t.arguments.any { a -> a.type?.resolve()?.let { mentionsDeprecated(it) } == true }

    private fun mentions(t: KSType, names: Set<String>): Boolean =
        (t.declaration is KSTypeParameter && t.declaration.simpleName.asString() in names) ||
            t.arguments.any { a -> a.type?.resolve()?.let { mentions(it, names) } == true }
}

/** KSP's mapToJvmSignature omits the array marker of a Java varargs parameter; the class file has it. */
private fun varargsFixed(descriptor: String, lastIsVarargs: Boolean): String {
    if (!lastIsVarargs) return descriptor
    val params = descriptor.substring(1, descriptor.indexOf(')'))
    val parts = Regex("\\[*(?:L[^;]+;|[BCDFIJSZ])").findAll(params).map { it.value }.toMutableList()
    if (parts.last().startsWith("[")) return descriptor
    parts[parts.lastIndex] = "[" + parts.last()
    return "(" + parts.joinToString("") + descriptor.substring(descriptor.indexOf(')'))
}

/** A Java type parameter's implicit Object bound is Kotlin's default Any?, not Any. */
private fun typeVar(p: KSTypeParameter, resolver: TypeParameterResolver): TypeVariableName {
    val tv = p.toTypeVariableName(resolver)
    return TypeVariableName(tv.name, tv.bounds.filterNot { it.copy(nullable = false) == ANY })
}

private val READ_ONLY = mapOf(MUTABLE_LIST to LIST, MUTABLE_MAP to MAP, MUTABLE_SET to SET, MUTABLE_COLLECTION to COLLECTION)

/** Java collections come back as flexible (Mutable)List types; expose the read-only Kotlin type. */
private fun readOnly(t: TypeName): TypeName =
    if (t is ParameterizedTypeName && t.rawType in READ_ONLY) READ_ONLY.getValue(t.rawType).parameterizedBy(t.typeArguments) else t

private fun List<CodeBlock>.joinToCode(): CodeBlock = CodeBlock.builder().apply {
    forEachIndexed { i, c -> if (i > 0) add(", "); add(c) }
}.build()
