package ru.yandex.practicum.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;

@Slf4j
@Service
public class SensorEventServiceImpl implements SensorEventService {
    @Override
    public void collect(SensorEvent event) {
        log.info("Получено событие датчика: {}", event);
    }
}
