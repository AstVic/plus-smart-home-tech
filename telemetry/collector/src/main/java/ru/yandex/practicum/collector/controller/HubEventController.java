package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.collector.service.HubEventService;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class HubEventController {
    private final HubEventService hubEventService;

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent hubEvent) {
        hubEventService.collect(hubEvent);
    }
}
