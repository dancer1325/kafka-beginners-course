package com.github.simplesteph.kafka.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer extends ElasticSearchClient{

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    //To create a Consumer as the one created in ConsumerDemoGroups
    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        //By default the commit of the offsets is at least once
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets, and we will do it manually
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); //To control the # of records to receive/request

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

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        //Topic's name is the one indicated in the TwitterProducer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        //Poll for new data as in the ConsumerDemoGroups
        while(true){
            //There are several ways to consume from Kafka and afterwards to store it in ES
            //1º) Using IndexRequest (firstWayToConsumeAndIntroducingInES)
            //firstWayToConsumeAndIntroducingInES(consumer, client);
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            //To accumulate a batch to do several requests at the same time
            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records){

                // 2 strategies to create a unique ID per record
                // 1º) kafka generic ID. It's the way to identify univocally an offset in Kafka
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                try {
                    //2º)To use the id_str field contained into the record.value
                    String id = extractIdFromTweet(record.value());

                    // where we insert data into ElasticSearch

                    //IndexRequest. When it already exists to index documents there.
                    /**
                     * Uncomment this code if you are using elastic search version < 7.0
                    IndexRequest indexRequest = new IndexRequest(
                       "twitter", //Index
                       "tweets", //Type
                       id // this is to make our consumer idempotent
                    ).source(record.value(), XContentType.JSON);
                    */
                    
                    /** Added for ElasticSearch >= v7.0
                     * The API to add data into ElasticSearch has slightly changes
                     * and therefore we must use this new method. 
                     * the idea is still the same
                     * read more here: https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
                     * uncomment the code above if you use ElasticSearch 6.x or less
                    */

                    IndexRequest indexRequest = new IndexRequest("tweets")
                       .source(record.value(), XContentType.JSON) //The data to insert are the Record's value which belongs to the Consumer
                       .id(id); // this is to make our consumer idempotent, in order the fact that ES can identify if you are doing several
                    //times the same request with the same source
//                  IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT); //It's faster to accumulate a batch to do several requests at the same time
//                  The next sleep thread isn't necessary since it's not done the client.index per request
//                    try {
//                        //In order not to consume all the entire topic immediately, to introduce a small delay
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }

                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
                } catch (NullPointerException e){ //To catch the case in which we get an error getting the id_src from the resource's source to insert in ES
                    logger.warn("skipping bad data: " + record.value());
                }

            }

            //To control the case in which we haven't got records
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync(); //To commit the offsets sync, after buffer all the records === out of the loop for
                logger.info("Offsets have been committed");
                try {
                    //In order not to consume all the entire topic immediately, to introduce a small delay
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void firstWayToConsumeAndIntroducingInES(KafkaConsumer<String, String> consumer, RestHighLevelClient client) throws IOException {
        ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
        for (ConsumerRecord<String, String> record : records){
            IndexRequest indexRequest = new IndexRequest(
                    "twitter", //Index
                    "tweets" //Type
            ).source(record.value(), XContentType.JSON);
            //We aren't adding ID in the request --> Since by default the commit offset is AtLeastOnce --> We could be doing several requests to ES
            //with the same source in the same index
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            logger.info(indexResponse.getId());
        }

    }
}
