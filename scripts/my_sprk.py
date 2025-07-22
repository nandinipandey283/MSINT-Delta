from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MNIST Test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("✅ Spark Session Started")
print("Spark Version:", spark.version)

df = spark.createDataFrame([
    (1, "Nandini"),
    (2, "MNIST"),
    (3, "Delta Lake")
], ["id", "label"])

df.show()

spark.stop()
