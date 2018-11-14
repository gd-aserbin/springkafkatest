package me.aserbin;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.ContainerAwareBatchErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication(exclude = {KafkaAutoConfiguration.class})
@Configuration
@EnableKafka
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(new Class[] { Main.class}, args);

        // check plain poll
    }

    public Map<String, Object> defaultKafkaProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tvs-kafka:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, String> myConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                defaultKafkaProperties(),
                new StringDeserializer(),
                new StringDeserializer());
    }


    @Bean
    public KafkaListenerContainerFactory<?> myContainerFactory(
            DefaultKafkaConsumerFactory<String, String> consumerFactory,
            @Value("${import.kafka.sku.concurrency:1}") int concurrency) {
        return createContainerFactory(consumerFactory, concurrency);
    }

    @Bean
    public TestListener testListener() {
        return new TestListener();
    }

    private <T, R> KafkaListenerContainerFactory<?> createContainerFactory(
            DefaultKafkaConsumerFactory<T, R> consumerFactory,
            int concurrency) {
        ConcurrentKafkaListenerContainerFactory<T, R> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.setBatchListener(true);
        factory.getContainerProperties().setBatchErrorHandler(new ContainerAwareBatchErrorHandler() {
            @Override
            public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
                container.stop();
                container.start();
            }
        });
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
