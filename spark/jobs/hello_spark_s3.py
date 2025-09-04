from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hello-spark-s3").getOrCreate()

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
print("Row count:", df.count())

out = "s3a://sports/tmp/hello_spark_s3/"
df.write.mode("overwrite").parquet(out)
print("Wrote parquet to", out)

spark.stop()