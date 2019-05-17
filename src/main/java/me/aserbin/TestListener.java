package me.aserbin;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.List;

@KafkaListener(
        topics = "test-poll2019",
        id = "poll-consumer",
        groupId = "test-poll-container",
        containerFactory = "myContainerFactory")
public class TestListener {

    @KafkaHandler
    public void listen(@Payload List<String> records,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                       @Header(KafkaHeaders.OFFSET) List<Long> offsets,
                       Acknowledgment acks) {

        records.forEach(System.out::println);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("(");
//        try {
//            acks.acknowledge();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
