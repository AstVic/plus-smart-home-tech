package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.analyzer.processor.SnapshotProcessor;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(Analyzer.class);
        application.setRegisterShutdownHook(false);
        ConfigurableApplicationContext context = application.run(args);

        final HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                snapshotProcessor.stop();
            } finally {
                try {
                    hubEventProcessor.stop();
                } finally {
                    context.close();
                }
            }
        }, "AnalyzerShutdownHook"));

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        snapshotProcessor.start();
    }
}
