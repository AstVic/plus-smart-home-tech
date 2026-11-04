package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

public interface HubEventService {
    void collect(HubEvent event);

    void collect(HubEventProto event);
}
