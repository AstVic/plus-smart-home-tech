package ru.yandex.practicum.analyzer.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.analyzer.deserializer.HubEventDeserializer;
import ru.yandex.practicum.analyzer.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Configuration
public class KafkaConfig {
    @Bean
    public Consumer<String, HubEventAvro> hubEventConsumer(KafkaProperties properties) {
        return new KafkaConsumer<>(consumerProperties(properties, properties.getHubEvents(), HubEventDeserializer.class));
    }

    @Bean
    public Consumer<String, SensorsSnapshotAvro> snapshotConsumer(KafkaProperties properties) {
        return new KafkaConsumer<>(consumerProperties(properties, properties.getSnapshots(), SensorsSnapshotDeserializer.class));
    }

    private Properties consumerProperties(KafkaProperties kafka, KafkaProperties.ConsumerProperties consumer,
                                          Class<?> deserializer) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumer.getGroupId());
        properties.putAll(consumer.getProperties());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        return properties;
    }
}
