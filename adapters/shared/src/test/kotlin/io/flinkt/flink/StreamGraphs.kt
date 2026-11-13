package io.flinkt.flink

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamNode
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.streaming.api.operators.UserFunctionProvider

/**
 * The structure of a stream graph, which the adapter contract compares between a pipeline built through the views and
 * the same one built with direct Flink calls (docs/testing.md, "Flink adapter contract").
 *
 * Node IDs come from a global counter, so a node is identified by its position in ID order, which is the order its
 * transformation was created in. Functions are compared by identity: both pipelines pass the same function objects, so
 * a node that got another function, or none, differs.
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
        val outputSerializer: TypeSerializer<*>?,
        val keySelectors: List<KeySelector<*, *>?>,
        val keySerializer: TypeSerializer<*>?,
        val edges: List<Edge>,
    )

    /** An edge to the node at position [target], with its partitioner. */
    data class Edge(val target: Int, val partitioner: Class<*>, val typeNumber: Int)

    /** The structure of [env]'s stream graph. Generating it clears [env]'s transformations, as Flink's own call does. */
    fun structure(env: StreamExecutionEnvironment): List<Node> {
        val nodes = env.streamGraph.streamNodes.sortedBy { it.id }
        val position = nodes.withIndex().associate { (i, node) -> node.id to i }
        return nodes.map { node ->
            val factory = node.operatorFactory
            val operator = (factory as? SimpleOperatorFactory<*>)?.operator
            Node(
                name = node.operatorName,
                factory = factory?.javaClass,
                operator = operator?.javaClass,
                function = (operator as? UserFunctionProvider<*>)?.userFunction,
                parallelism = node.parallelism,
                maxParallelism = maxParallelism(node),
                inputSerializers = node.typeSerializersIn.toList(),
                outputSerializer = node.typeSerializerOut,
                keySelectors = node.statePartitioners.toList(),
                keySerializer = node.stateKeySerializer,
                edges = node.outEdges.map { Edge(position.getValue(it.targetId), it.partitioner.javaClass, it.typeNumber) },
            )
        }
    }

    /** The transformations [env] holds, which entering a view mustn't change. Reading them reads no stream's type. */
    fun transformations(env: StreamExecutionEnvironment): List<Transformation<*>> = env.transformations.toList()

    // StreamNode.getMaxParallelism() is package-private on 1.20 and public on 2.x.
    private fun maxParallelism(node: StreamNode): Int =
        StreamNode::class.java.getDeclaredMethod("getMaxParallelism").apply { isAccessible = true }.invoke(node) as Int
}
