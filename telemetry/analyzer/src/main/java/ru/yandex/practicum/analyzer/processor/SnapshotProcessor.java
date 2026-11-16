package ru.yandex.practicum.analyzer.processor;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaProperties;
import ru.yandex.practicum.analyzer.service.HubRouterClient;
import ru.yandex.practicum.analyzer.service.ScenarioService;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class SnapshotProcessor {
    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final KafkaProperties properties;
    private final ScenarioService scenarioService;
    private final HubRouterClient hubRouterClient;
    private final Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile Thread processingThread;

    public SnapshotProcessor(@Qualifier("snapshotConsumer") Consumer<String, SensorsSnapshotAvro> consumer,
                             KafkaProperties properties, ScenarioService scenarioService, HubRouterClient hubRouterClient) {
        this.consumer = consumer;
        this.properties = properties;
        this.scenarioService = scenarioService;
        this.hubRouterClient = hubRouterClient;
    }

    public void start() {
        processingThread = Thread.currentThread();
        String topic = properties.getSnapshots().getTopic();
        try {
            consumer.subscribe(List.of(topic));
            log.info("Обработчик подписан на топик {}", topic);
            while (running.get()) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(properties.getPollTimeout());
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    handle(record);
                    processedOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    commitProcessedOffsets();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Ошибка обработки сообщений топика " + topic, e);
        } finally {
            running.set(false);
            try {
                try {
                    commitProcessedOffsets();
                } finally {
                    consumer.close();
                }
            } finally {
                stopped.countDown();
                log.info("Обработчик топика {} остановлен", topic);
            }
        }
    }

    @PreDestroy
    public void stop() {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
        }
        if (processingThread != null && processingThread != Thread.currentThread()) {
            try {
                if (!stopped.await(properties.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                    log.warn("Обработчик не завершился за {}", properties.getShutdownTimeout());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Ожидание остановки обработчика прервано", e);
            }
        }
    }

    private void handle(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        if (record.value() == null) {
            return;
        }
        try {
            hubRouterClient.execute(scenarioService.findActions(record.value()));
        } catch (Exception e) {
            log.error("Ошибка обработки снапшота хаба {}: топик {}, партиция {}, смещение {}",
                    record.value().getHubId(), record.topic(), record.partition(), record.offset(), e);
        }
    }

    private void commitProcessedOffsets() {
        if (!processedOffsets.isEmpty()) {
            consumer.commitSync(new HashMap<>(processedOffsets));
            processedOffsets.clear();
        }
    }
}
