from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("View Delta Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("delta_tables/mnist_digits")
df.show(truncate=False)

spark.stop()
