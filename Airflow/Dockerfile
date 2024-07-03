FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \ 
    apt-get install -y gcc python3-dev openjdk-11-jdk procps && \
    apt-get clean

# RUN apt install -y python3 python3-pip
# RUN pip3 install --upgrade pip setuptools --user
# RUN rm -r /root/.cache && rm -rf /var/cache/apt/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr

RUN curl -sSL https://archive.apache.org/dist/hadoop/core/hadoop-3.3.0/hadoop-3.3.0.tar.gz | tar -xz -C /usr/local/
ENV HADOOP_HOME /usr/local/hadoop-3.3.0

# Set Hadoop environment variables
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin

# Copy the MySQL connector JAR file
COPY mysql-connector-j-8.3.0.jar /usr/local/hadoop-3.3.0/share/hadoop/common/lib/
ENV SPARK_CLASSPATH /usr/local/hadoop-3.3.0/share/hadoop/common/lib/mysql-connector-j-8.3.0.jar

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark pymysql

COPY requirements.txt .

RUN pip install -r requirements.txt --user
