package me.aserbin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Producer {
    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tvs-kafka:9092");

        KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props,
                new StringSerializer(),
                new StringSerializer());

        for (int i = 0; i< 10;i++) {
            prod.send(new ProducerRecord<>("test-poll2019", 0,"1", new Date().toString()));
            prod.flush();
        }

    }
}
