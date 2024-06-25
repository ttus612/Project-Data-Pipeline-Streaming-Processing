# RUN KAFKA

### Go to the directory of Kafka

### START ZOOKEEPER (Open new terminal):
``` bin/zookeeper-server-start.sh config/zookeeper.properties ```

### START KAFKA SERVER (Open new terminal):
```  bin/kafka-server-start.sh config/server.properties ```  

### CREATE KAFKA TOPIC - KAFKA TOPIC NAME: "test1"
``` bin/kafka-topics.sh --create --topic test1 --bootstrap-server localhost:9092 ```

### SEND LOG DATA TO KAFKA TOPIC THROUGH KAFKA PRODUCER (GRENERATED DATA)
``` Run script "Kafka/to_Cassandra/Producer_to_Kafka.py" ```

### READ DATA FROM KAFKA TOPIC AND STORAGE IN DATA LAKE: CASSANDRA 
``` Run script "Kafka/to_Cassandra/Consumert_to_Cassandra.py" ```