package ru.yandex.practicum.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.aggregator.service.AggregationStarter;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AggregatorApplication {
    public static void main(String[] args) {
        // Запускаем Spring Boot и получаем настроенный контекст приложения.
        ConfigurableApplicationContext context = SpringApplication.run(AggregatorApplication.class, args);

        // Получаем бин AggregationStarter и запускаем основную логику сервиса.
        AggregationStarter aggregator = context.getBean(AggregationStarter.class);
        aggregator.start();
    }
}
