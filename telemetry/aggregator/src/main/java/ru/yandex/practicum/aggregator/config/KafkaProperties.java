package ru.yandex.practicum.aggregator.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator.kafka")
public class KafkaProperties {
    private String bootstrapServers;
    private String consumerGroupId;
    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Map<String, String> topics = new HashMap<>();
    private Map<String, String> consumerProperties = new HashMap<>();
    private Map<String, String> producerProperties = new HashMap<>();
}
