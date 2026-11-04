package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

public interface HubEventService {
    void collect(HubEventProto event);
}
