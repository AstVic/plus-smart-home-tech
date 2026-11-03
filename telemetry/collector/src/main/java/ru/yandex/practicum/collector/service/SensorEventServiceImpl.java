package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.config.KafkaProperties;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventServiceImpl implements SensorEventService {
    private final SensorEventMapper sensorEventMapper;
    private final Producer<String, SpecificRecordBase> kafkaProducer;
    private final KafkaProperties kafkaProperties;

    @Override
    public void collect(SensorEvent event) {
        SensorEventAvro sensorEventAvro = sensorEventMapper.toAvro(event);
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(
                        kafkaProperties.getTopics().get("sensors"),
                        null,
                        event.getTimestamp().toEpochMilli(),
                        event.getHubId(),
                        sensorEventAvro);
        kafkaProducer.send(record);
        log.info("Отправлено событие датчика в Kafka: {}", event);
    }
}
