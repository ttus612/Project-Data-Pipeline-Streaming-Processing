import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import subprocess
import logging

# def done():
#     return print(" NEW DATA")

# def false():
#     return print("No new data found")

# def check_data_callable():
#     result = subprocess.run(['python', 'jobs/python/checkdata.py'], capture_output=True, text=True)
#     logging.info(f"checkdata.py output: {result.stdout.strip()}")
#     return 'false_job' if result.returncode == 'False' else 'done_job'

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

# done_job = PythonOperator(
#     task_id='done_job',
#     python_callable=done,
#     dag=dag,
# )

# false_job = PythonOperator(
#     task_id='false_job',
#     python_callable=false,
#     dag=dag,
# )   

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

# check_data = BranchPythonOperator(
#     task_id='check_data',
#     python_callable=check_data_callable,
#     dag=dag,
# )

check_data = SparkSubmitOperator(
    task_id="check_data",
    conn_id="spark-conn",
    application="jobs/python/checkdata.py",
    packages="com.datastax.spark:spark-cassandra-connector_2.12:3.1.0",
    jars='/usr/local/hadoop-3.3.0/share/hadoop/common/lib/mysql-connector-j-8.3.0.jar',
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

# connect_cassandra = SparkSubmitOperator(
#     task_id="cassandra_job",
#     conn_id="spark-conn",
#     application="jobs/python/connectdb_cassandra.py",
#     packages="com.datastax.spark:spark-cassandra-connector_2.12:3.1.0",
#     dag=dag
# )

# connect_mysql = SparkSubmitOperator(
#     task_id="mysql_job",
#     conn_id="spark-conn",
#     application="jobs/python/connectdb_mysql.py",
#     jars='/usr/local/hadoop-3.3.0/share/hadoop/common/lib/mysql-connector-j-8.3.0.jar',
#     dag=dag
# )

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

# start >> python_job >> connect_cassandra >> connect_mysql >> python_job_etl >> end
# Write the code to run the jobs in the correct order

start >> check_data >> python_job >> python_job_etl >> end
