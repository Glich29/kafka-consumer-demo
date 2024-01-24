package org.sbislava;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerDemoApp {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerDemoApp.class.getSimpleName());
    private static final String KAFKA_HOST = "127.0.0.1:9092";
    private static final String TOPIC = "kafka.demo.with.key";
    private static final String CONSUMER_GROUP_ID = "consumer-group-1";

    public static void main( String[] args ) {
        log.info("Kafka consumer demo app started...");
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //subscribe to a topic
        consumer.subscribe(Collections.singletonList(TOPIC));

        //poll for data
        while (true) {
            log.info("Polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                log.info("Key: " + record.key() + " - value: " + record.value());
                log.info("Partition: " + record.partition() + " - offset: " + record.offset());
            });
        }
    }
}
