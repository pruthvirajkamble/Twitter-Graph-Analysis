import org.apache.spark.graphx._

// Create a GraphX graph
val graph = GraphLoader.edgeListFile(sc, "../data/graphx/higgs-retweet_network.edgelist")

// Calculate PageRank scores
val pageRank = graph.pageRank(0.85)

val connectedComponents = graph.connectedComponents()

// Combine PageRank scores and connected component information
val impactAnalysis = pageRank.vertices.innerJoin(connectedComponents.vertices) {
  (nodeId, pageRankScore, connectedComponentId) => (pageRankScore, connectedComponentId)
}

// Group the results by PageRank score and connected component size
val groupedResults = impactAnalysis.groupBy {
  case (_, (pageRankScore, connectedComponentId)) => (pageRankScore, connectedComponentId)
}.mapValues(iter => iter.size)

// Find the top 10 connected component IDs with the highest influential node count
val top10Components = groupedResults.top(10)(Ordering.by(_._2))

// Print the top 10 connected component IDs and their node counts
top10Components.foreach {
  case ((pageRankScore, connectedComponentId), nodeCount) =>
    println(s"PageRank Score: $pageRankScore, Connected Component ID: $connectedComponentId, Node Count: $nodeCount")
}

// Stop Spark when done
sc.stop()