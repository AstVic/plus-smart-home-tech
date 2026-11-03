package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.model.sensor.SensorEvent;

public interface SensorEventService {
    void collect(SensorEvent event);
}
