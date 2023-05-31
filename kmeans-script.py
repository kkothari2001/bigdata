from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('spark://192.168.1.115:7077', 'test')
spark = SparkSession(sc)
# Loads data.
dataset = spark.read.format("libsvm").load("file:///home/kmk/spark-3.3.2-bin-hadoop3/data/mllib/sample_kmeans_data.txt")

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

