from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("invoices").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("invoice_id", StringType()),
    StructField("customer", StringType()),
    StructField("amount", DoubleType()),
    StructField("issued_at", TimestampType()),
    StructField("due_days", IntegerType()),
    StructField("paid_late", BooleanType()),
])

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:19092")
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()
      .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
      .select("d.*"))

df.writeStream.format("console").option("truncate", "false").start().awaitTermination()