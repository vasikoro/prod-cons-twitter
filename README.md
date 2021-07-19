

# Content
- Basics of Kafka
- Twitter Producer
- ElasticSearch Consumer




SETUP:
if you are not setting up kafka for the first time run:
sudo bin/zookeeper-server-stop.sh
to ensure tha you have a clean start 

STEP1:
bin/zookeeper-server-start.sh config/zookeeper.properties

STEP2:
bin/kafka-server-start.sh config/server.properties

STEP3:
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092

STEP4:
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092



Elasticsearch:
/_cat/indices?v