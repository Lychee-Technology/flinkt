package io.flinkt.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamNode
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.streaming.api.operators.UserFunctionProvider
import org.apache.flink.streaming.api.transformations.OneInputTransformation

/**
 * The structure of a stream graph, which the adapter contract compares between a pipeline built through the views and
 * the same one built with direct Flink calls (docs/testing.md, "Flink adapter contract").
 *
 * Node IDs come from a global counter, so a node is identified by its position in ID order, which is the order its
 * transformation was created in. Functions are compared by identity: both pipelines pass the same function objects, so
 * a node that got another function, or none, differs.
 *
 * A node's output type and key type are the `TypeInformation` its transformation holds, compared by equality, as Flink
 * compares them. They're recorded beside the serializers because two different `TypeInformation`s can create equal
 * serializers, so the serializers alone would miss an operator that was given the wrong type.
 */
object StreamGraphs {
    data class Node(
        val name: String,
        val factory: Class<*>?,
        val operator: Class<*>?,
        val function: Any?,
        val parallelism: Int,
        val maxParallelism: Int,
        val inputSerializers: List<TypeSerializer<*>?>,
        val outputType: TypeInformation<*>,
        val outputSerializer: TypeSerializer<*>?,
        val keySelectors: List<KeySelector<*, *>?>,
        val keyType: TypeInformation<*>?,
        val keySerializer: TypeSerializer<*>?,
        val edges: List<Edge>,
    )

    /** An edge to the node at position [target], with its partitioner. */
    data class Edge(val target: Int, val partitioner: Class<*>, val typeNumber: Int)

    /** The structure of [env]'s stream graph. Generating it clears [env]'s transformations, as Flink's own call does. */
    fun structure(env: StreamExecutionEnvironment): List<Node> {
        // Read before the graph is generated, which clears env's transformations.
        val transformations = transformationsById(env)
        val nodes = env.streamGraph.streamNodes.sortedBy { it.id }
        val position = nodes.withIndex().associate { (i, node) -> node.id to i }
        return nodes.map { node ->
            val factory = node.operatorFactory
            val operator = (factory as? SimpleOperatorFactory<*>)?.operator
            val transformation = transformations[node.id]
                ?: error("Node ${node.id}, ${node.operatorName}, has no transformation with its ID")
            Node(
                name = node.operatorName,
                factory = factory?.javaClass,
                operator = operator?.javaClass,
                function = (operator as? UserFunctionProvider<*>)?.userFunction,
                parallelism = node.parallelism,
                maxParallelism = maxParallelism(node),
                inputSerializers = node.typeSerializersIn.toList(),
                outputType = transformation.outputType,
                outputSerializer = node.typeSerializerOut,
                keySelectors = node.statePartitioners.toList(),
                keyType = (transformation as? OneInputTransformation<*, *>)?.stateKeyType,
                keySerializer = node.stateKeySerializer,
                edges = node.outEdges.map { Edge(position.getValue(it.targetId), it.partitioner.javaClass, it.typeNumber) },
            )
        }
    }

    /**
     * Every transformation [env]'s graph is generated from, by ID: those [env] holds and their predecessors, such as
     * the source. A stream node that a transformation translates to has the transformation's ID, and a partitioning
     * step, which becomes an edge, has no node.
     */
    private fun transformationsById(env: StreamExecutionEnvironment): Map<Int, Transformation<*>> =
        env.transformations.flatMap { it.transitivePredecessors }.associateBy { it.id }

    /** The transformations [env] holds, which entering a view mustn't change. Reading them reads no stream's type. */
    fun transformations(env: StreamExecutionEnvironment): List<Transformation<*>> = env.transformations.toList()

    // StreamNode.getMaxParallelism() is package-private on 1.20 and public on 2.x.
    private fun maxParallelism(node: StreamNode): Int =
        StreamNode::class.java.getDeclaredMethod("getMaxParallelism").apply { isAccessible = true }.invoke(node) as Int
}
