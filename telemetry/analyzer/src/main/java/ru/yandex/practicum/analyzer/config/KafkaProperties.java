package ru.yandex.practicum.analyzer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ConfigurationProperties(prefix = "analyzer.kafka")
public class KafkaProperties {
    private String bootstrapServers;
    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private ConsumerProperties hubEvents = new ConsumerProperties();
    private ConsumerProperties snapshots = new ConsumerProperties();

    @Getter
    @Setter
    public static class ConsumerProperties {
        private String topic;
        private String groupId;
        private Map<String, String> properties = new HashMap<>();
    }
}
