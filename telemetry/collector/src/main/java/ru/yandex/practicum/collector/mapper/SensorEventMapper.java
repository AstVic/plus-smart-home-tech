package ru.yandex.practicum.collector.mapper;

import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;

@Component
public class SensorEventMapper {

    public SensorEventAvro toAvro(SensorEventProto event) {
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(toInstant(event.getTimestamp()))
                .setPayload(toPayload(event))
                .build();
    }

    private Object toPayload(SensorEventProto event) {
        return switch (event.getPayloadCase()) {
            case CLIMATE_SENSOR_EVENT -> toAvro(event.getClimateSensorEvent());
            case LIGHT_SENSOR_EVENT -> toAvro(event.getLightSensorEvent());
            case MOTION_SENSOR_EVENT -> toAvro(event.getMotionSensorEvent());
            case SWITCH_SENSOR_EVENT -> toAvro(event.getSwitchSensorEvent());
            case TEMPERATURE_SENSOR_EVENT -> toAvro(event, event.getTemperatureSensorEvent());
            default -> throw new IllegalArgumentException("Неизвестный тип события датчика: " + event.getPayloadCase());
        };
    }

    private ClimateSensorAvro toAvro(ClimateSensorProto event) {
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setHumidity(event.getHumidity())
                .setCo2Level(event.getCo2Level())
                .build();
    }

    private LightSensorAvro toAvro(LightSensorProto event) {
        return LightSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setLuminosity(event.getLuminosity())
                .build();
    }

    private MotionSensorAvro toAvro(MotionSensorProto event) {
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setMotion(event.getMotion())
                .setVoltage(event.getVoltage())
                .build();
    }

    private SwitchSensorAvro toAvro(SwitchSensorProto event) {
        return SwitchSensorAvro.newBuilder()
                .setState(event.getState())
                .build();
    }

    private TemperatureSensorAvro toAvro(SensorEventProto event, TemperatureSensorProto temperature) {
        return TemperatureSensorAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(toInstant(event.getTimestamp()))
                .setTemperatureC(temperature.getTemperatureC())
                .setTemperatureF(temperature.getTemperatureF())
                .build();
    }

    private Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
