from pyspark.sql import SparkSession
from graphframes import GraphFrame
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]

spark = SparkSession.builder.appName("SocialNetworkAnalysis").getOrCreate()
spark.sparkContext.setCheckpointDir('/checkpoints')

edges_df = spark.read.option("delimiter", ",").csv(input_file, header=True, inferSchema=True)

from_vertices = edges_df.select("src").distinct().withColumnRenamed("src", "id")
to_vertices = edges_df.select("dst").distinct().withColumnRenamed("dst", "id")

vertices_df = from_vertices.union(to_vertices).distinct()

social_graph = GraphFrame(vertices_df, edges_df)

# a. Find the top 5 nodes with the highest outdegree and the count of outgoing edges
out_degree = social_graph.outDegrees
top_out_degree_nodes = out_degree.orderBy("outDegree", ascending=False).limit(5)
top_out_degree_nodes.toPandas().to_csv(output_file, mode='a', index=False)

# b. Find the top 5 nodes with the highest indegree and count of incoming edges
in_degree = social_graph.inDegrees
top_in_degree_nodes = in_degree.orderBy("inDegree", ascending=False).limit(5)
top_in_degree_nodes.toPandas().to_csv(output_file, mode='a', index=False)

# c. Calculate PageRank for each node and output the top 5 nodes
pagerank = social_graph.pageRank(resetProbability=0.15, tol=0.01)
top_pagerank_nodes = pagerank.vertices.orderBy("pagerank", ascending=False).limit(5)
top_pagerank_nodes.toPandas().to_csv(output_file, mode='a', index=False)

# d. Run the connected components algorithm and find the top 5 components with the largest number of nodes
connected_components = social_graph.connectedComponents()
top_components = connected_components.groupBy("component").count().orderBy("count", ascending=False).limit(5)
top_components.toPandas().to_csv(output_file, mode='a', index=False)

# e. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest
# triangle count
triangle_counts = social_graph.triangleCount()
top_triangles = triangle_counts.orderBy("count", ascending=False).limit(5)
top_triangles.toPandas().to_csv(output_file, mode='a', index=False)
