from pyspark.sql import SparkSession
import mysql.connector
from mysql.connector import Error
from pyspark.sql.utils import AnalysisException
import sys

spark = SparkSession.builder.config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'study_data_engineering').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time(url,driver,user,password):    
    sql = """(select max(Last_Updated_At) from events_etl) data"""
    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    mysql_time = mysql_time.take(1)[0][0]
    
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        # mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
        mysql_latest = mysql_time
    return mysql_latest

def create_mysql_table_if_not_exists(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS events_etl (
        job_id INT,
        date DATE,
        hour INT,
        publisher_id INT,
        company_id INT,
        campaign_id INT,
        group_id INT,
        unqualified INT,
        qualified INT,
        conversion INT,
        click INT,
        bid_set VARCHAR(255),
        spend_hour DECIMAL(10, 2),
        Last_Updated_At VARCHAR(255),
        sources VARCHAR(255)
    );
    """
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()

def check_mysql_table_exists(connection):
    check_table_query = """
    SHOW TABLES LIKE 'events_etl';
    """
    cursor = connection.cursor()
    cursor.execute(check_table_query)
    result = cursor.fetchone()
    return result is not None

def main():
    host = 'mysql'
    port = '3306'
    db_name = 'Data_Warehouse'
    user = 'root'
    password = '1'
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"

    
    connection = mysql.connector.connect(
        host=host,
        port=port,
        database=db_name,
        user=user,
        password=password
    )

    cassandra_time = get_latest_time_cassandra()
    print('--------------------------------------------------')
    print("Connected to Cassandra database")
    print('Cassandra latest time is {}'.format(cassandra_time))
    print('--------------------------------------------------')

    # Check if MySQL table exists, create if not
    if not check_mysql_table_exists(connection):
        print("MySQL table 'events_etl' does not exist. Creating...")
        create_mysql_table_if_not_exists(connection)
    else:
        print("MySQL table 'events_etl' already exists.")
    
    mysql_time = get_mysql_latest_time(url, driver, user, password)
    print('MySQL latest time is {}'.format(mysql_time))
    print('--------------------------------------------------')

    if cassandra_time > mysql_time : 
        print('New data is available in Cassandra')
        spark.stop()
        return 'True'      
    else:
        print('No new data available in Cassandra')
        spark.stop()
        return 'False'            
   
if __name__ == "__main__":
    main()