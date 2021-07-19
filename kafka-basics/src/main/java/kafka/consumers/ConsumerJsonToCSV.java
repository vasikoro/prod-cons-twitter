package kafka.consumers;
import com.github.opendevl.JFlat;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerJsonToCSV {

    static Logger logger = LoggerFactory.getLogger(ConsumerJsonToCSV.class.getName());


    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson){
        // gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws Exception {

        List tweets = new ArrayList();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000)); // new in Kafka 2.0.0

            Integer recordCount = records.count();

            for (ConsumerRecord<String, String> record : records){

                // 2 strategies
                // kafka generic ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                try {
                    String id = extractIdFromTweet(record.value());
                    String tweetBody = record.value().replaceAll("\"", "");
//                    String tweetBody = record.value().replaceAll("^\"|\"$\"\"", "");
                    System.out.println(id + tweetBody);

                    JFlat flatMe = new JFlat(record.value());

                    //get the 2D representation of JSON document
//                    List<Object[]> json2csv = flatMe.json2Sheet().getJsonAsSheet();

                    //get the 2D representation of JSON document
                    flatMe.json2Sheet().headerSeparator("_").getJsonAsSheet();

                    //write the 2D representation in csv format
                    String path = "/home/vasiko/workspace/prod-cons-twitter/kafka-basics/Data/";
                    path = path + id;
                    path = path + ".csv";
                    flatMe.write2csv(path);

                } catch (NullPointerException e){
                    logger.warn("skipping bad data: " + record.value());
                }

            }

            if (recordCount > 0) {
                logger.info("Committing offsets...");

                consumer.commitSync();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }

        // close the client gracefully
        // client.close();



    }
}
