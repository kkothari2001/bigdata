var df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:9000/lab10/Clean_Dataset.csv")
df.show()
df.printSchema()

df.columns
df.count()
df.columns.size


df.describe().show()
var final_df = df.select("source_city", "destination_city", "duration", "price")
var vertices_df = final_df.select("source_city").union(final_df.select("destination_city")).distinct()
vertices_df = vertices_df.withColumn("id", monotonically_increasing_id + 0)
vertices_df.cache()


vertices_df.show()
var vertices_rdd = vertices_df.rdd
vertices_rdd.collect()
var kv_rdd = vertices_df.rdd.map(row => (row.getAs[Long](1),(row.getAs[String](0))))
kv_rdd.collect()
import org.apache.spark.graphx._
var joined = final_df.join(vertices_df, Seq("source_city"), "left_outer")
joined.show()
import org.apache.spark.sql.functions._
joined = joined.select($"destination_city", $"duration", $"price", $"id".alias("origin")).drop("id")
vertices_df = vertices_df.select($"source_city".alias("destination_city"),$"id").drop("source_city")
vertices_df.show()
joined = joined.join(vertices_df, Seq("destination_city"), "left_outer")
joined = joined.select($"origin", $"id".alias("destination"), $"duration", $"price").drop("id")
joined.show()
var edges_rdd1 = joined.rdd.map(row => Edge(row.getAs[Long]("origin"), row.getAs[Long]("destination"), (row.getAs[Int]("price"), row.getAs[Double]("duration"))))
edges_rdd1.collect()
val nowhere="nowhere"
val graph = Graph(kv_rdd, edges_rdd1, nowhere)
graph.vertices.collect.take(100)
graph.edges.collect.take(100)
println(s"Number of Flight Routes: ${graph.numEdges} \n")
println(s"Number of Airports: ${graph.numVertices} \n")
var sortedEdges = graph.edges.distinct().sortBy(edge => -edge.attr._2)
println("Longest Routes (time):")
sortedEdges.take(100)
sortedEdges.take(20).foreach(edge => println(s"Source: ${kv_rdd.lookup(edge.srcId)(0)}; Destination: ${kv_rdd.lookup(edge.dstId)(0)}; Cost: ${edge.attr._1}; Duration: ${edge.attr._2} h"))
println("\n")
println("Indegrees of each Vertex (Airport):")
graph.inDegrees.collect()
println("\n")
var inDegrees = graph.inDegrees.distinct().sortBy(-1*_._2)
println("Vertices (Airports) with highest Indegrees:")
inDegrees.take(100)
println("\n")
println(s"Airport with highest Indegree vertex: ${kv_rdd.lookup(inDegrees.take(1)(0)._1)(0)} with an Indegree of ${inDegrees.take(1)(0)._2}")
println("\n")
var pgrank = graph.pageRank(0.00001)
pgrank.vertices.sortBy(-_._2).collect()
println(s"The Airport with highest PageRank is ${kv_rdd.lookup(pgrank.vertices.sortBy(-_._2).take(1)(0)._1)(0)} and has a PageRank value of ${pgrank.vertices.sortBy(-_._2).take(1)(0)._2}")
println("\n")
var sortedPrices = graph.edges.distinct().sortBy(edge => edge.attr._1)
println("Routes with lowest cost:")
sortedPrices.take(100)
sortedPrices.take(20).foreach(edge => println(s"Source: ${kv_rdd.lookup(edge.srcId)(0)}; Destination: ${kv_rdd.lookup(edge.dstId)(0)}; Cost: ${edge.attr._1}; Duration: ${edge.attr._2} h"))
sys.exit