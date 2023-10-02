import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._


val retweetGraph = GraphLoader.edgeListFile(sc, "retweet_network.edgelist")
val mentionGraph = GraphLoader.edgeListFile(sc, "mention_network.edgelist")

// Combine the retweet and mention graphs by merging their vertices and edges
val combinedGraph = Graph(
  retweetGraph.vertices.union(mentionGraph.vertices),
  retweetGraph.edges.union(mentionGraph.edges)
).cache()

// Run PageRank on the combined influence network
val pageRank = combinedGraph.pageRank(0.85).vertices

// Find the user with the highest PageRank score
val mostInfluentialPerson = pageRank.max()(Ordering.by(_._2))

// Print the most influential person
println(s"The most influential person is User ${mostInfluentialPerson._1} with PageRank ${mostInfluentialPerson._2}")