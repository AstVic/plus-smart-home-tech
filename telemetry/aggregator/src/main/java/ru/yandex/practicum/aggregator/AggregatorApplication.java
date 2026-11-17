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
        SpringApplication application = new SpringApplication(AggregatorApplication.class);
        application.setRegisterShutdownHook(false);
        try (ConfigurableApplicationContext context = application.run(args)) {
            AggregationStarter aggregationStarter = context.getBean(AggregationStarter.class);
            Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                aggregationStarter.stop();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    context.close();
                }
            }, "AggregatorShutdownHook"));

            aggregationStarter.start();
        }
    }
}
