package ru.yandex.practicum.analyzer.processor;

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
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {
    private final Consumer<String, HubEventAvro> consumer;
    private final KafkaProperties properties;
    private final HubEventService hubEventService;
    private final Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile Thread processingThread;

    public HubEventProcessor(@Qualifier("hubEventConsumer") Consumer<String, HubEventAvro> consumer,
                             KafkaProperties properties, HubEventService hubEventService) {
        this.consumer = consumer;
        this.properties = properties;
        this.hubEventService = hubEventService;
    }

    @Override
    public void run() {
        processingThread = Thread.currentThread();
        String topic = properties.getHubEvents().getTopic();
        try {
            consumer.subscribe(List.of(topic));
            log.info("Обработчик подписан на топик {}", topic);
            while (running.get()) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(properties.getPollTimeout());
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    handle(record);
                    processedOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                commitProcessedOffsets();
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

    public void stop() {
        running.set(false);
        consumer.wakeup();
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

    private void handle(ConsumerRecord<String, HubEventAvro> record) {
        if (record.value() == null) {
            return;
        }
        try {
            hubEventService.process(record.value());
        } catch (Exception e) {
            log.error("Ошибка обработки события хаба: топик {}, партиция {}, смещение {}",
                    record.topic(), record.partition(), record.offset(), e);
        }
    }

    private void commitProcessedOffsets() {
        if (!processedOffsets.isEmpty()) {
            consumer.commitSync(new HashMap<>(processedOffsets));
            processedOffsets.clear();
        }
    }
}
