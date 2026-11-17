package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Action;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.Condition;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.analyzer.model.Sensor;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Transactional
    public void process(HubEventAvro event) {
        String hubId = event.getHubId();
        switch (event.getPayload()) {
            case DeviceAddedEventAvro added -> addDevice(hubId, added);
            case DeviceRemovedEventAvro removed -> removeDevice(hubId, removed.getId());
            case ScenarioAddedEventAvro added -> saveScenario(hubId, added);
            case ScenarioRemovedEventAvro removed -> scenarioRepository.findByHubIdAndName(hubId, removed.getName())
                    .ifPresent(this::removeScenario);
            default -> throw new IllegalArgumentException("Неизвестное событие хаба: " + event.getPayload());
        }
    }

    private void addDevice(String hubId, DeviceAddedEventAvro event) {
        sensorRepository.findById(event.getId()).ifPresentOrElse(sensor -> {
            if (!sensor.getHubId().equals(hubId)) {
                log.warn("Устройство {} уже зарегистрировано в другом хабе", event.getId());
            }
        }, () -> sensorRepository.save(new Sensor(event.getId(), hubId)));
    }

    private void removeDevice(String hubId, String sensorId) {
        sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
            scenarioRepository.findByHubIdAndSensorId(hubId, sensorId).forEach(this::removeScenario);
            sensorRepository.delete(sensor);
        });
    }

    private void saveScenario(String hubId, ScenarioAddedEventAvro event) {
        Set<String> sensorIds = Stream.concat(
                event.getConditions().stream().map(condition -> condition.getSensorId()),
                event.getActions().stream().map(action -> action.getSensorId())
        ).collect(Collectors.toSet());
        Map<String, Sensor> sensors = sensorRepository.findAllByIdInAndHubId(sensorIds, hubId).stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));
        if (sensors.size() != sensorIds.size()) {
            log.warn("Сценарий {} хаба {} содержит незарегистрированные устройства или устройства другого хаба",
                    event.getName(), hubId);
            return;
        }

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, event.getName()).orElseGet(Scenario::new);
        if (scenario.getId() != null) {
            clearDefinition(scenario);
        }
        scenario.setHubId(hubId);
        scenario.setName(event.getName());

        event.getConditions().forEach(source -> {
            Condition condition = new Condition();
            condition.setType(ConditionType.valueOf(source.getType().name()));
            condition.setOperation(ConditionOperation.valueOf(source.getOperation().name()));
            condition.setValue(toInteger(source.getValue()));
            scenario.getConditions().add(new ScenarioCondition(sensors.get(source.getSensorId()),
                    conditionRepository.save(condition)));
        });
        event.getActions().forEach(source -> {
            Action action = new Action();
            action.setType(ActionType.valueOf(source.getType().name()));
            action.setValue(source.getValue());
            scenario.getActions().add(new ScenarioAction(sensors.get(source.getSensorId()), actionRepository.save(action)));
        });
        scenarioRepository.save(scenario);
    }

    private void removeScenario(Scenario scenario) {
        clearDefinition(scenario);
        scenarioRepository.delete(scenario);
        scenarioRepository.flush();
    }

    private void clearDefinition(Scenario scenario) {
        List<Condition> conditions = scenario.getConditions().stream().map(ScenarioCondition::getCondition).toList();
        List<Action> actions = scenario.getActions().stream().map(ScenarioAction::getAction).toList();
        scenario.getConditions().clear();
        scenario.getActions().clear();
        scenarioRepository.flush();
        conditionRepository.deleteAll(conditions);
        actionRepository.deleteAll(actions);
    }

    private Integer toInteger(Object value) {
        return switch (value) {
            case null -> null;
            case Integer number -> number;
            case Boolean flag -> flag ? 1 : 0;
            default -> throw new IllegalArgumentException("Неподдерживаемое значение условия: " + value);
        };
    }
}
