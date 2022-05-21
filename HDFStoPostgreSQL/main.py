import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark_hdfs = SparkSession.builder.master("yarn").appName("hdfs_test").getOrCreate()
titanic_df = spark_hdfs.read.format("csv").option("header", True).option("separator", ",")\
    .load("hdfs:///user/teran45/data/titanic.csv")
titanic_df.select("Name", "Sex", "Age").write.format("jdbc").option("url", "jdbc:postgresql://localhost/spark_labs") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "titanic") \
    .option("user", "teran45").option("password", "Bodyart56").save()

spark_hdfs.stop()
