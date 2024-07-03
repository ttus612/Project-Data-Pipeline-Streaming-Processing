from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()

df_company = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://mysql:3306/Data_Warehouse") \
    .option("dbtable", "company") \
    .option("user", "root") \
    .option("password", "1") \
    .load()

df_company.show()

spark.stop()