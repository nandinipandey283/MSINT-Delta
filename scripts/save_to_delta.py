import os
import random
from PIL import Image
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MNIST Delta Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "flat_files/mnistpng/training"
data = []

for label in os.listdir(base_path):
    label_path = os.path.join(base_path, label)
    if os.path.isdir(label_path):
        all_images = os.listdir(label_path)
        random_images = random.sample(all_images, 5)
        for img_file in random_images:
            img_path = os.path.join(label_path, img_file)
            img = Image.open(img_path).convert("L")
            pixels = list(img.getdata())
            data.append((img_file, label, pixels))

df = pd.DataFrame(data, columns=["filename", "label", "pixels"])
spark_df = spark.createDataFrame(df)

spark_df.write.format("delta") \
    .mode("overwrite") \
    .save("delta_tables/mnist_digits")
