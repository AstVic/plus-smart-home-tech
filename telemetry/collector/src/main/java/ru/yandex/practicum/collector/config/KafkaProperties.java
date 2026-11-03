package ru.yandex.practicum.collector.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "collector.kafka")
@Getter
@Setter
public class KafkaProperties {
    private String bootstrapServers;
    private Map<String, String> topics = new HashMap<>();
}

