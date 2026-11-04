package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorEventService {
    void collect(SensorEvent event);

    void collect(SensorEventProto event);
}
