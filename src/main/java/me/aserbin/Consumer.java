package me.aserbin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Consumer {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("o").setLevel(Level.WARNING);
        Logger.getLogger("o.a.k").setLevel(Level.WARNING);
        Logger.getLogger("kafka").setLevel(Level.WARNING);


        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tvs-kafka:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        KafkaConsumer<String, String> cons = new KafkaConsumer<String, String>(props,
                new StringDeserializer(),
                new StringDeserializer());

        cons.subscribe(Collections.singletonList("test-poll"));

        while (true) {
            int count = 0;
            System.out.println("--------------- MESSAGES -------------------------");
            for (ConsumerRecord<String, String> rec : cons.poll(100)) {
                System.out.println(rec.value());
                count++;
            }

            Thread.sleep(1000);


//            if (count > 0) {
//                cons.commitSync();
//                System.out.println("committed");
//            }
//            System.out.println(cons.position(new TopicPartition("test-offsets-retention", 1)));
////            System.out.println(cons.committed(new TopicPartition("test-offsets-retention", 1)).offset());
//            Thread.sleep(10_000);
        }

    }
}
