package ru.yandex.practicum.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Action;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ScenarioService {
    private final ScenarioRepository scenarioRepository;
    private final ConditionEvaluator conditionEvaluator;

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public List<DeviceActionRequest> findActions(SensorsSnapshotAvro snapshot) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        return scenarioRepository.findByHubId(snapshot.getHubId()).stream()
                .filter(scenario -> !scenario.getConditions().isEmpty())
                .filter(scenario -> scenario.getConditions().stream().allMatch(condition ->
                        conditionEvaluator.matches(condition.getCondition(),
                                snapshot.getSensorsState().get(condition.getSensor().getId()))))
                .flatMap(scenario -> scenario.getActions().stream()
                        .map(action -> toRequest(scenario, action, timestamp)))
                .toList();
    }

    private DeviceActionRequest toRequest(Scenario scenario, ScenarioAction scenarioAction, Timestamp timestamp) {
        Action action = scenarioAction.getAction();
        DeviceActionProto.Builder deviceAction = DeviceActionProto.newBuilder()
                .setSensorId(scenarioAction.getSensor().getId())
                .setType(ActionTypeProto.valueOf(action.getType().name()));
        if (action.getValue() != null) {
            deviceAction.setValue(action.getValue());
        }
        return DeviceActionRequest.newBuilder()
                .setHubId(scenario.getHubId())
                .setScenarioName(scenario.getName())
                .setAction(deviceAction)
                .setTimestamp(timestamp)
                .build();
    }
}
