package ru.yandex.practicum.analyzer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "analyzer.hub-router")
public class HubRouterProperties {
    private Duration requestTimeout = Duration.ofSeconds(5);
}
