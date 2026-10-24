package ru.yandex.practicum.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.model.hub.HubEvent;

@Slf4j
@Service
public class HubEventServiceImpl implements HubEventService {
    @Override
    public void collect(HubEvent event) {
        log.info("Получено событие хаба: {}", event);
    }
}
