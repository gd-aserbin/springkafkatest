package me.aserbin;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.List;

@KafkaListener(
        topics = "test-poll",
        id = "poll-consumer",
        groupId = "test-poll-container",
        containerFactory = "myContainerFactory")
public class TestListener {

    @KafkaHandler
    public void listen(@Payload(required = false) List<String> message,
                       Acknowledgment ack, Consumer<String, String> consumer) {
        message.forEach(System.out::println);

        // emulate re-reads with exception

        throw new RuntimeException();
    }
}
