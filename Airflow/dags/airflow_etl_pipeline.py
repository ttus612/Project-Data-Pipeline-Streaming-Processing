import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "sparking_flow",
    default_args = {
        "owner": "THANH TU",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

python_job_etl = SparkSubmitOperator(
    task_id="python_job_etl",
    conn_id="spark-conn",
    application="jobs/python/pyspark_etl_auto.py",
    packages="com.datastax.spark:spark-cassandra-connector_2.12:3.1.0",
    jars='/usr/local/hadoop-3.3.0/share/hadoop/common/lib/mysql-connector-j-8.3.0.jar',
    dag=dag

)

connect_cassandra = SparkSubmitOperator(
    task_id="cassandra_job",
    conn_id="spark-conn",
    application="jobs/python/connectdb_cassandra.py",
    packages="com.datastax.spark:spark-cassandra-connector_2.12:3.1.0",
    dag=dag
)

connect_mysql = SparkSubmitOperator(
    task_id="mysql_job",
    conn_id="spark-conn",
    application="jobs/python/connectdb_mysql.py",
    jars='/usr/local/hadoop-3.3.0/share/hadoop/common/lib/mysql-connector-j-8.3.0.jar',
    dag=dag

)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> connect_cassandra >> connect_mysql >> python_job_etl >> end