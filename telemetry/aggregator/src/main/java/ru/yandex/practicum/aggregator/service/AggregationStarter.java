package ru.yandex.practicum.aggregator.service;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

    public void start() {
        boolean commitPending = false;

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            String sensorTopic = getRequiredTopic(SENSOR_TOPIC_KEY);
            consumer.subscribe(List.of(sensorTopic));
            log.info("Aggregator подписан на топик {}", sensorTopic);

            while (true) {
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
        } catch (WakeupException ignored) {
            // Закрываем консьюмер и продюсер в блоке finally.
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                producer.flush();
                if (commitPending) {
                    consumer.commitSync();
                }
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
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
