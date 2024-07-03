from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

spark_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='tracking',keyspace='study_data_engineering').load()

spark_df.printSchema()

spark_df.show()

data = spark_df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
data = data.filter(data.job_id.isNotNull())
data.show()

spark.stop()