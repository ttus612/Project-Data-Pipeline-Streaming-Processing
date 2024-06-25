from datetime import datetime, timedelta
import time
import random
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
import datetime
import mysql.connector
from kafka import KafkaProducer
import json
from cassandra.cqlengine.models import  Model
import cassandra

host = 'localhost'
port = '3306'
db_name = 'Data_Warehouse'
user = 'root'
password = '1'
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"

kafka_bootstrap_severs = "localhost:9092"
producer = KafkaProducer(bootstrap_servers= kafka_bootstrap_severs, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
kafka_topic = "test1"


def get_data_from_job(user,password,host,database):
    cnx = mysql.connector.connect(user=user, password=password,
                                         host=host,
                                      database=database)
    query = """select id as job_id,campaign_id , group_id , company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def get_data_from_publisher(user,password,host,database):
    cnx = mysql.connector.connect(user=user, password=password,
                                         host=host,
                                      database=database)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data


def generating_dummy_data(n_records):
    publisher = get_data_from_publisher(user,password,host,db_name)
    publisher = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job(user,password,host,db_name)
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    company_list = jobs_data['company_id'].to_list()
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    i = 0 
    fake_records = n_records
    while i <= fake_records:
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # sql = """ INSERT INTO tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES ('{}',{},{},'{}',{},{},{},'{}')""".format(create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts)
        data = {
            "create_time": create_time,
            "bid":bid,
            "campaign_id":campaign_id,
            "custom_track":custom_track,
            "group_id":group_id,
            "job_id":job_id,
            "publisher_id":publisher_id,
            "ts":ts,
        }
        print(data)
        producer.send(kafka_topic, value=data)
        # print(sql)
        # session.execute(sql)
        i+=1 
    return print("Data Generated Successfully")

status = "ON"
while status == "ON":
    generating_dummy_data(n_records = random.randint(1,5))
    time.sleep(1)
producer.close()