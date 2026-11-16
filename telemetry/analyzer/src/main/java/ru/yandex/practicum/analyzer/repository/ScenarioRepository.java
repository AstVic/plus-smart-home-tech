package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.analyzer.model.Scenario;

import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {
    List<Scenario> findByHubId(String hubId);

    Optional<Scenario> findByHubIdAndName(String hubId, String name);

    @Query("""
            SELECT DISTINCT s FROM Scenario s
            LEFT JOIN s.conditions c
            LEFT JOIN s.actions a
            WHERE s.hubId = :hubId AND (c.sensor.id = :sensorId OR a.sensor.id = :sensorId)
            """)
    List<Scenario> findByHubIdAndSensorId(@Param("hubId") String hubId, @Param("sensorId") String sensorId);
}
