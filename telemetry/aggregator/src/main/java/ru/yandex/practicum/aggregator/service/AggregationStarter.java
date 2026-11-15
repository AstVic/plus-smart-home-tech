package ru.yandex.practicum.aggregator.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private static final String SENSOR_TOPIC_KEY = "sensors";
    private static final String SNAPSHOT_TOPIC_KEY = "snapshots";

    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SensorsSnapshotAvro> producer;
    private final KafkaProperties kafkaProperties;
    private final SnapshotService snapshotService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile Thread processingThread;

    public void start() {
        boolean commitPending = false;
        processingThread = Thread.currentThread();
        running.set(true);

        try {
            String sensorTopic = getRequiredTopic(SENSOR_TOPIC_KEY);
            consumer.subscribe(List.of(sensorTopic));
            log.info("Aggregator подписан на топик {}", sensorTopic);

            while (running.get()) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(kafkaProperties.getPollTimeout());
                if (records.isEmpty()) {
                    continue;
                }

                List<Future<RecordMetadata>> acknowledgements = new ArrayList<>();
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    snapshotService.updateState(record.value())
                            .ifPresent(snapshot -> acknowledgements.add(sendSnapshot(snapshot)));
                }

                awaitDelivery(acknowledgements);
                commitPending = true;
                consumer.commitSync();
                commitPending = false;
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Ошибка во время обработки событий от датчиков", e);
        } finally {
            running.set(false);
            try {
                closeResources(commitPending);
            } finally {
                stopped.countDown();
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
                if (!stopped.await(kafkaProperties.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                    log.warn("Не удалось завершить агрегацию за {}", kafkaProperties.getShutdownTimeout());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Ожидание завершения агрегации прервано", e);
            }
        }
    }

    private void closeResources(boolean commitPending) {
        try {
            producer.flush();
            if (commitPending) {
                consumer.commitSync();
            }
        } finally {
            try {
                log.info("Закрываем консьюмер");
                consumer.close();
            } finally {
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private Future<RecordMetadata> sendSnapshot(SensorsSnapshotAvro snapshot) {
        ProducerRecord<String, SensorsSnapshotAvro> record = new ProducerRecord<>(
                getRequiredTopic(SNAPSHOT_TOPIC_KEY),
                null,
                snapshot.getTimestamp().toEpochMilli(),
                snapshot.getHubId(),
                snapshot
        );
        return producer.send(record);
    }

    private void awaitDelivery(List<Future<RecordMetadata>> acknowledgements) {
        try {
            for (Future<RecordMetadata> acknowledgement : acknowledgements) {
                acknowledgement.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Отправка снапшота была прервана", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Не удалось отправить снапшот в Kafka", e.getCause());
        }
    }

    private String getRequiredTopic(String key) {
        String topic = kafkaProperties.getTopics().get(key);
        if (topic == null || topic.isBlank()) {
            throw new IllegalStateException("Не настроен Kafka-топик aggregator.kafka.topics." + key);
        }
        return topic;
    }
}
