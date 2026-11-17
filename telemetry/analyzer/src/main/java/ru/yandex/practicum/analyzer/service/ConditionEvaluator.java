package ru.yandex.practicum.analyzer.service;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.model.Condition;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Component
public class ConditionEvaluator {
    public boolean matches(Condition condition, SensorStateAvro state) {
        if (state == null || condition.getValue() == null) {
            return false;
        }
        Integer actual = extractValue(condition.getType(), state.getData());
        return actual != null && condition.getOperation().matches(actual, condition.getValue());
    }

    private Integer extractValue(ConditionType type, Object data) {
        return switch (type) {
            case MOTION -> data instanceof MotionSensorAvro sensor ? (sensor.getMotion() ? 1 : 0) : null;
            case LUMINOSITY -> data instanceof LightSensorAvro sensor ? sensor.getLuminosity() : null;
            case SWITCH -> data instanceof SwitchSensorAvro sensor ? (sensor.getState() ? 1 : 0) : null;
            case HUMIDITY -> data instanceof ClimateSensorAvro sensor ? sensor.getHumidity() : null;
            case CO2LEVEL -> data instanceof ClimateSensorAvro sensor ? sensor.getCo2Level() : null;
            case TEMPERATURE -> switch (data) {
                case TemperatureSensorAvro sensor -> sensor.getTemperatureC();
                case ClimateSensorAvro sensor -> sensor.getTemperatureC();
                default -> null;
            };
        };
    }
}
