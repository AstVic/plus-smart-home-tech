package ru.yandex.practicum.analyzer.service;

import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.config.HubRouterProperties;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class HubRouterClient {
    private final HubRouterControllerBlockingStub hubRouterClient;
    private final HubRouterProperties properties;

    public HubRouterClient(@GrpcClient("hub-router") HubRouterControllerBlockingStub hubRouterClient,
                           HubRouterProperties properties) {
        this.hubRouterClient = hubRouterClient;
        this.properties = properties;
    }

    public void execute(List<DeviceActionRequest> actions) {
        for (DeviceActionRequest action : actions) {
            try {
                hubRouterClient.withDeadlineAfter(properties.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .handleDeviceAction(action);
            } catch (StatusRuntimeException e) {
                log.error("Не удалось отправить действие сценария {} хаба {} устройству {}: {}",
                        action.getScenarioName(), action.getHubId(), action.getAction().getSensorId(), e.getStatus(), e);
            }
        }
    }
}
