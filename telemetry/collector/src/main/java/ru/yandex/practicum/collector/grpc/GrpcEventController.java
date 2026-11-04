package ru.yandex.practicum.collector.grpc;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@GrpcService
public class GrpcEventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
       try {
           log.info("Пришло событие типа: {}", request.getPayloadCase());
           responseObserver.onNext(Empty.getDefaultInstance());
           responseObserver.onCompleted();
       } catch (Exception e) {
           responseObserver.onError(new StatusRuntimeException(
                   Status.INTERNAL
                           .withDescription(e.getLocalizedMessage())
                           .withCause(e)
           ));
       }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Пришло событие типа: {}", request.getPayloadCase());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }
}
