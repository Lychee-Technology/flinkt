package io.flinkt.codegen

import com.google.devtools.ksp.KspExperimental
import com.google.devtools.ksp.getAllSuperTypes
import com.google.devtools.ksp.getConstructors
import com.google.devtools.ksp.getDeclaredFunctions
import com.google.devtools.ksp.isPublic
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
import com.google.devtools.ksp.symbol.KSFile
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSType
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.Modifier
import java.io.File

/** Creates the [FlinkViewProcessor]. Registered in `META-INF/services`. */
public class FlinkViewProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor =
        FlinkViewProcessor(environment.codeGenerator, environment.logger, environment.options)
}

/**
 * Generates the forwarders of each [FlinkView] (docs/architecture.md, "Generated forwarders").
 *
 * It takes two KSP options, which the adapter's build passes:
 * - [FLINK_API_FILE], the path of the adapter's `flink-api.tsv`, whose parameter names the forwarders take;
 * - [FLINK_VERSION], the version of the Flink on the compile classpath, which the names file has to have been
 *   extracted from.
 *
 * The forwarders depend on every view, because a returned stream is wrapped in the view of its class, so each
 * generated file is aggregating, with every view as its origin.
 */
internal class FlinkViewProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
    private val options: Map<String, String>,
) : SymbolProcessor {
    private var done = false

    override fun process(resolver: Resolver): List<KSAnnotated> {
        // Everything happens in the first round. The forwarders it generates are what the views' supertypes need.
        if (done) return emptyList()
        done = true
        val api = flinkApi() ?: return emptyList()
        val views = resolver.getSymbolsWithAnnotation(FLINK_VIEW).mapNotNull(::view).toList()
        if (views.isEmpty()) return emptyList()
        for ((flinkClass, duplicates) in views.groupBy { it.flinkClassName }.filterValues { it.size > 1 }) {
            for (view in duplicates) {
                logger.error("${duplicates.joinToString { it.simpleName }} all view $flinkClass: a Flink class has one view", view.declaration)
            }
        }
        val viewOf = views.associateBy { it.flinkClassName }
        val origins = views.mapNotNull { it.declaration.containingFile }.distinct()
        // A view's forwarders extend those of the view of its Flink class's superclass, so parents go first.
        for (view in views.sortedBy { it.flinkClass.depth() }) {
            write(view, Generation(resolver, api, view, viewOf).generate(), origins)
        }
        return emptyList()
    }

    /** The names file, checked against the Flink version on the classpath. Null, with an error logged, if it fails. */
    private fun flinkApi(): FlinkApi? {
        val path = options[FLINK_API_FILE]
        val classpathVersion = options[FLINK_VERSION]
        if (path == null || classpathVersion == null) {
            logger.error(
                "flinkt-view-codegen needs the KSP options $FLINK_API_FILE, the path of the adapter's flink-api.tsv, " +
                    "and $FLINK_VERSION, the version of the Flink on the compile classpath. The adapter's build passes both.",
            )
            return null
        }
        val file = File(path)
        val api = try {
            FlinkApi.parse(file.readText(), path)
        } catch (e: java.io.IOException) {
            logger.error("Can't read the names file $path: $e")
            return null
        } catch (e: IllegalArgumentException) {
            logger.error("The names file is malformed: ${e.message}")
            return null
        }
        if (api.flinkVersion != classpathVersion) {
            logger.error(
                "The names file $path was extracted from Flink ${api.flinkVersion}, but the classpath has Flink " +
                    "$classpathVersion. Extract it again from Flink $classpathVersion's class files with the adapter's " +
                    "extractFlinkApi task.",
            )
            return null
        }
        return api
    }

    private fun view(symbol: KSAnnotated): View? {
        val declaration = symbol as? KSClassDeclaration
        if (declaration == null || declaration.classKind != ClassKind.CLASS) {
            logger.error("@FlinkView applies to classes only", symbol)
            return null
        }
        val annotation = declaration.annotations.first { it.qualifiedName() == FLINK_VIEW }
        val flinkClass = (annotation.argument("flinkClass") as? KSType)?.declaration as? KSClassDeclaration
        if (flinkClass == null || flinkClass.classKind != ClassKind.CLASS) {
            logger.error("@FlinkView names the Flink class the view holds, such as DataStream::class", declaration)
            return null
        }
        val forward = (annotation.argument("forward") as? List<*>).orEmpty().map { it.toString() }
        val view = View(declaration, flinkClass, forward)
        if (declaration.typeParameters.size != flinkClass.typeParameters.size) {
            logger.error(
                "${view.simpleName} has ${declaration.typeParameters.size} type parameters, but ${flinkClass.simpleName.asString()} " +
                    "has ${flinkClass.typeParameters.size}: a view has the type parameters of the Flink class it holds, in order",
                declaration,
            )
            return null
        }
        val wraps = declaration.getConstructors().any { constructor ->
            constructor.parameters.size == 1 &&
                constructor.parameters.single().type.resolve().declaration.qualifiedName() == view.flinkClassName
        }
        if (!wraps) {
            logger.error(
                "${view.simpleName} needs a constructor that takes the ${flinkClass.simpleName.asString()} it views: " +
                    "the generated forwarders wrap what Flink returns with it",
                declaration,
            )
            return null
        }
        forward.groupingBy { it }.eachCount().filterValues { it > 1 }.keys.forEach { name ->
            logger.error("${view.simpleName} lists $name more than once", declaration)
        }
        return view
    }

    private fun write(view: View, source: String, origins: List<KSFile>) {
        codeGenerator.createNewFile(Dependencies(true, *origins.toTypedArray()), view.packageName, view.forwardersSimpleName)
            .bufferedWriter()
            .use { it.write(source) }
    }

    /** The forwarders of one view. Generation logs an error for each listed method that breaks a view rule. */
    private inner class Generation(
        private val resolver: Resolver,
        private val api: FlinkApi,
        private val view: View,
        private val viewOf: Map<String, View>,
    ) {
        private val flinkVersion = api.flinkVersion
        private val flinkClass = view.flinkClass

        /** How each Flink class above [flinkClass] writes its type parameters, in the view's terms. */
        private val classScope: Map<String, String> = supertypeScope()

        /** The views of [flinkClass]'s superclasses, nearest first. */
        private val ancestors: List<View> = generateSequence(flinkClass.superclass()) { it.superclass() }
            .mapNotNull { viewOf[it.qualifiedName()] }
            .toList()

        /** The nearest Flink superclass of [flinkClass] that has a view, whose forwarders the generated ones extend. */
        private val parent: View? = ancestors.firstOrNull()

        /** What the views above forward, which a forwarder with the same JVM name and parameters overrides. */
        private val inherited: Set<String> by lazy { ancestors.flatMapTo(mutableSetOf()) { it.signatures } }

        /**
         * The source of the generated interface. A refused method is left out of it and logged as an error, which fails
         * the build. The interface is still written, so that the build reports the refusals, not the unresolved
         * supertypes of views that would follow from a missing interface.
         */
        fun generate(): String {
            val members = view.forward.flatMap(::forwarders)
            view.signatures += members.map { it.signature }
            return source(members)
        }

        private fun forwarders(name: String): List<Forwarder> {
            val overloads = methods(name)
            if (overloads.isEmpty()) {
                return fail(
                    "${view.simpleName} forwards $name, but ${flinkClass.simpleName.asString()} has no public method $name " +
                        "in Flink $flinkVersion. Remove it from forward, or write it in the source set of the lines that have it.",
                )
            }
            // A method of the parent's Flink class, or above it, is the parent view's to forward.
            val parentClasses = parent?.flinkClass?.let { setOf(it.qualifiedName()) + it.getAllSuperTypes().map { t -> t.declaration.qualifiedName() } }.orEmpty()
            val own = overloads.filter { (it.parentDeclaration as? KSClassDeclaration)?.qualifiedName() !in parentClasses }
            if (own.isEmpty()) {
                val owner = (overloads.first().parentDeclaration as KSClassDeclaration).simpleName.asString()
                return fail(
                    "${view.simpleName} forwards $name, which $owner declares. ${parent!!.simpleName} views $owner, and " +
                        "${view.forwardersSimpleName} extends ${parent.forwardersSimpleName}: forward $name there.",
                )
            }
            return own.mapNotNull(::forwarder).sortedBy { it.signature }
        }

        private fun forwarder(method: KSFunctionDeclaration): Forwarder? {
            val owner = method.parentDeclaration as KSClassDeclaration
            val signature = describe(method, owner)
            if (method.hasAnnotation(INTERNAL)) {
                return failOne("$signature is @Internal in Flink $flinkVersion, so it isn't public API, in Java either. A view doesn't forward it.")
            }
            val returnType = method.returnType!!.resolve()
            val types = method.parameters.map { it.type.resolve() } + returnType
            types.firstOrNull { it.hasError() }?.let { type ->
                return failOne("$signature uses ${describe(type)}, which isn't on the compile classpath, so its forwarder can't be written.")
            }
            val methodTypeParameters = method.typeParameters.map { it.name.asString() }.toSet()
            if (returnType.mentions(methodTypeParameters)) {
                return failOne(
                    "$signature returns ${describe(returnType)}, a type built from its type parameter: a view can't " +
                        "forward a method that introduces an element type. Write it by hand, with a reified type parameter.",
                )
            }
            val returnedView = viewOf[returnType.declaration.qualifiedName()]
            if (returnedView == null) {
                returnType.streamTypeWithoutView()?.let { stream ->
                    return failOne(
                        "$signature returns ${describe(returnType)}, and ${stream.simpleName.asString()} is a Flink stream " +
                            "type that has no view: forwarding it would let a chain leave the view without asFlink().",
                    )
                }
            }
            val descriptor = descriptor(method)
            val names = api.find(owner.binaryName(), method.simpleName.asString(), descriptor)?.parameterNames
                ?: return failOne(
                    "$signature has no parameter names in the names file: it has no row for ${owner.binaryName()} " +
                        "${method.simpleName.asString()} $descriptor. Extract the file again from Flink $flinkVersion's " +
                        "class files with the adapter's extractFlinkApi task.",
                )
            val scope = Scope(classScope, method.typeParameters, view.typeParameterNames)
            val parameters = method.parameters.mapIndexed { i, parameter -> Parameter(parameter, names[i], scope) }
            val call = "asFlink().${identifier(method.simpleName.asString())}(${parameters.joinToString { it.argument }})"
            val nullable = method.hasAnnotationNamed("Nullable")
            val (type, body) = when {
                returnType.declaration.qualifiedName() == "kotlin.Unit" -> null to "{\n        $call\n    }"
                returnedView != null -> {
                    val wrapped = qualified(returnedView.qualifiedName) + scope.arguments(returnType)
                    wrapped to "=\n        ${qualified(returnedView.qualifiedName)}($call)"
                }
                else -> scope.render(returnType.makeNotNullable()) + (if (nullable || returnType.isMarkedNullable) "?" else "") to "=\n        $call"
            }
            val jvmSignature = jvmSignature(method)
            return Forwarder(
                signature = jvmSignature,
                source = buildString {
                    if (method.hasAnnotation(DEPRECATED) || method.hasAnnotation(KOTLIN_DEPRECATED)) {
                        append("    @Deprecated(\"Deprecated in Flink: ${owner.nestedName()}.${method.simpleName.asString()}\")\n")
                    }
                    if (method.hasAnnotation(EXPERIMENTAL)) append("    @$EXPERIMENTAL_FLINK_API\n")
                    append("    public ")
                    if (jvmSignature in inherited) append("override ")
                    append("fun ")
                    scope.declarations().takeIf { it.isNotEmpty() }?.let { append('<').append(it).append("> ") }
                    append(identifier(method.simpleName.asString()))
                    append('(').append(parameters.joinToString { it.declaration }).append(')')
                    if (type != null) append(": ").append(type)
                    scope.constraints().takeIf { it.isNotEmpty() }?.let { append(" where ").append(it) }
                    append(' ').append(body).append('\n')
                },
            )
        }

        /**
         * The public instance methods named [name] of [flinkClass], each as the class that declares it declares it:
         * [flinkClass]'s own, then its superclasses', nearest first, then its interfaces'. A method hides one it
         * overrides. KSP's own member list gives an inherited Java method the subclass as its parent, but the names
         * file, the view of the class that declares it and its type parameters all go by the declaring class.
         */
        private fun methods(name: String): List<KSFunctionDeclaration> {
            val classes = generateSequence(flinkClass) { it.superclass() }.toList()
            val interfaces = flinkClass.getAllSuperTypes()
                .mapNotNull { it.declaration as? KSClassDeclaration }
                .filter { it.classKind == ClassKind.INTERFACE }
            val hidden = mutableSetOf<String>()
            return (classes + interfaces).flatMap { type ->
                type.getDeclaredFunctions().filter { it.simpleName.asString() == name && it.isPublic() && Modifier.JAVA_STATIC !in it.modifiers }
            }.filter { method ->
                val visible = jvmSignature(method) !in hidden
                if (visible) {
                    hidden += jvmSignature(method)
                    generateSequence(method.findOverridee() as? KSFunctionDeclaration) { it.findOverridee() as? KSFunctionDeclaration }
                        .forEach { hidden += jvmSignature(it) }
                }
                visible
            }
        }

        /** The JVM name and parameter descriptors of [method], such as `union([L…/DataStream;)`, which an override shares. */
        private fun jvmSignature(method: KSFunctionDeclaration): String =
            method.simpleName.asString() + descriptor(method).substringBefore(')') + ")"

        /** The class file's descriptor of [method], with the varargs array marker that KSP drops put back. */
        @OptIn(KspExperimental::class)
        private fun descriptor(method: KSFunctionDeclaration): String {
            val descriptor = resolver.mapToJvmSignature(method) ?: error("KSP has no JVM signature for $method")
            val last = method.parameters.lastOrNull()
            return if (last != null && last.isVararg) JvmDescriptors.withVarargsArray(descriptor, last.type.resolve().arrayDimensions()) else descriptor
        }

        private fun source(members: List<Forwarder>): String = buildString {
            append("// Generated by flinkt-view-codegen from ${view.flinkClassName}, Flink $flinkVersion. Don't edit it.\n")
            // A forwarder of a deprecated Flink method calls it, and one whose signature has a deprecated Flink type uses it.
            append("@file:Suppress(\"DEPRECATION\", \"OVERRIDE_DEPRECATION\")\n\n")
            if (view.packageName.isNotEmpty()) append("package ").append(qualified(view.packageName)).append("\n\n")
            val scope = Scope(classScope, emptyList(), view.typeParameterNames)
            val typeParameters = flinkClass.typeParameters.mapIndexed { i, parameter ->
                view.typeParameterNames[i] + parameter.bounds.map { it.resolve() }.filterNot { it.isAny() }.singleOrNull()?.let { ": ${scope.render(it)}" }.orEmpty()
            }
            append("public sealed interface ").append(identifier(view.forwardersSimpleName)).append('<').append(typeParameters.joinToString()).append('>')
            if (parent != null) {
                val arguments = parent.flinkClass.typeParameters.joinToString { classScope.getValue(it.key()) }
                append(" : ").append(qualified(parent.forwardersQualifiedName)).append('<').append(arguments).append('>')
            }
            append(" {\n")
            append("    public ").append(if (parent != null) "override " else "").append("fun asFlink(): ")
                .append(qualified(view.flinkClassName)).append('<').append(view.typeParameterNames.joinToString()).append(">\n")
            for (member in members) append('\n').append(member.source)
            append("}\n")
        }

        /**
         * Each type parameter of [flinkClass] and of its supertypes, written in the view's terms: [flinkClass]'s own are
         * the view's type parameters, in order, and a supertype's are the arguments the class below gives it.
         */
        private fun supertypeScope(): Map<String, String> {
            val scope = mutableMapOf<String, String>()
            flinkClass.typeParameters.forEachIndexed { i, parameter -> scope[parameter.key()] = view.typeParameterNames[i] }
            val queue = ArrayDeque(listOf(flinkClass))
            val visited = mutableSetOf<String>()
            while (queue.isNotEmpty()) {
                val type = queue.removeFirst()
                if (!visited.add(type.qualifiedName())) continue
                val render = Scope(scope.toMap(), emptyList(), emptyList())
                for (reference in type.superTypes) {
                    val supertype = reference.resolve()
                    val declaration = supertype.declaration as? KSClassDeclaration ?: continue
                    declaration.typeParameters.forEachIndexed { i, parameter ->
                        val argument = supertype.arguments.getOrNull(i)?.type?.resolve()
                        scope.putIfAbsent(parameter.key(), argument?.let(render::render) ?: "*")
                    }
                    queue += declaration
                }
            }
            return scope
        }

        /** The Flink stream type in [this] that has no view, if any: a Flink stream or stream-builder type, at any depth. */
        private fun KSType.streamTypeWithoutView(): KSClassDeclaration? {
            val declaration = declaration as? KSClassDeclaration ?: return null
            if (viewOf[declaration.qualifiedName()] == null && declaration.isStreamType()) return declaration
            return arguments.firstNotNullOfOrNull { it.type?.resolve()?.streamTypeWithoutView() }
        }

        /** A parameter of a forwarder: Flink's, named from the names file, and a view where Flink takes a stream that has one. */
        private inner class Parameter(parameter: KSValueParameter, name: String, scope: Scope) {
            private val vararg = parameter.isVararg

            // KSP gives a Java varargs parameter `T... values` its element type, T, as a Kotlin vararg declares it.
            private val type = parameter.type.resolve()
            private val nullable = type.isMarkedNullable || parameter.hasAnnotationNamed("Nullable")
            private val identifier = identifier(name)
            private val view = viewOf[type.declaration.qualifiedName()]

            val declaration: String = (if (vararg) "vararg " else "") + identifier + ": " +
                (view?.let { qualified(it.qualifiedName) + scope.arguments(type) } ?: scope.render(type.makeNotNullable())) +
                (if (nullable) "?" else "")

            val argument: String = when {
                view != null && vararg -> "*$identifier.map { it.asFlink() }.toTypedArray()"
                view != null -> identifier + (if (nullable) "?" else "") + ".asFlink()"
                vararg -> "*$identifier"
                else -> identifier
            }
        }

        private fun <T> fail(message: String): List<T> {
            logger.error(message, view.declaration)
            return emptyList()
        }

        private fun failOne(message: String): Forwarder? {
            fail<Forwarder>(message)
            return null
        }

        private fun describe(method: KSFunctionDeclaration, owner: KSClassDeclaration): String =
            "${owner.nestedName()}.${method.simpleName.asString()}(" +
                method.parameters.joinToString { describe(it.type.resolve()) + if (it.isVararg) "..." else "" } + ")"
    }

    private class View(val declaration: KSClassDeclaration, val flinkClass: KSClassDeclaration, val forward: List<String>) {
        val packageName: String = declaration.packageName.asString()
        val simpleName: String = declaration.simpleName.asString()
        val qualifiedName: String = declaration.qualifiedName()
        val flinkClassName: String = flinkClass.qualifiedName()
        val forwardersSimpleName: String = simpleName + "Forwarders"
        val forwardersQualifiedName: String = if (packageName.isEmpty()) forwardersSimpleName else "$packageName.$forwardersSimpleName"
        val typeParameterNames: List<String> = declaration.typeParameters.map { it.name.asString() }

        /** The JVM name and parameter descriptors of each forwarded method, which a subclass's view overrides. */
        val signatures = mutableSetOf<String>()
    }

    private class Forwarder(val signature: String, val source: String)

    companion object {
        const val FLINK_API_FILE = "flinkt.view.flinkApiFile"
        const val FLINK_VERSION = "flinkt.view.flinkVersion"

        private const val FLINK_VIEW = "io.flinkt.codegen.FlinkView"
        private const val EXPERIMENTAL_FLINK_API = "io.flinkt.flink.ExperimentalFlinkApi"
        private const val INTERNAL = "org.apache.flink.annotation.Internal"
        private const val EXPERIMENTAL = "org.apache.flink.annotation.Experimental"
        private const val DEPRECATED = "java.lang.Deprecated"
        private const val KOTLIN_DEPRECATED = "kotlin.Deprecated"
    }
}
