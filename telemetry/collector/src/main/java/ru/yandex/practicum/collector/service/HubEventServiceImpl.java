package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.config.KafkaProperties;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventServiceImpl implements HubEventService {
    private final HubEventMapper hubEventMapper;
    private final Producer<String, SpecificRecordBase> kafkaProducer;
    private final KafkaProperties kafkaProperties;

    @Override
    public void collect(HubEvent event) {
        HubEventAvro hubEventAvro = hubEventMapper.toAvro(event);
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(
                        kafkaProperties.getTopics().get("hubs"),
                        null,
                        event.getTimestamp().toEpochMilli(),
                        event.getHubId(),
                        hubEventAvro);
        kafkaProducer.send(record);
        log.info("Отправлено событие хаба в Kafka: {}", event);
    }
}
