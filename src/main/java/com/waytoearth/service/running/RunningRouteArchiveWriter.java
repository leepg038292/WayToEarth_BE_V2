package com.waytoearth.service.running;

import com.waytoearth.event.RunningRouteArchiveRequestedEvent.PointSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * running_route_archive에 대한 배치 upsert. JPA persist(개별 INSERT)가 아니라
 * JdbcTemplate.batchUpdate로 처리해서, 완료 시점에 한 러닝의 좌표 전체를 저비용으로 복제한다.
 * ON DUPLICATE KEY UPDATE로 멱등하게 처리하므로, 이벤트가 중복 발행되거나 재시도돼도 안전하다.
 */
@Component
@RequiredArgsConstructor
public class RunningRouteArchiveWriter {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
            INSERT INTO running_route_archive
            (running_record_id, sequence, latitude, longitude, timestamp_seconds, heart_rate,
             pace_seconds, altitude, accuracy, cumulative_distance_meters, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
             latitude = VALUES(latitude),
             longitude = VALUES(longitude),
             timestamp_seconds = VALUES(timestamp_seconds),
             heart_rate = VALUES(heart_rate),
             pace_seconds = VALUES(pace_seconds),
             altitude = VALUES(altitude),
             accuracy = VALUES(accuracy),
             cumulative_distance_meters = VALUES(cumulative_distance_meters),
             updated_at = VALUES(updated_at)
            """;

    public void archive(Long runningRecordId, List<PointSnapshot> points) {
        if (points.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(UPSERT_SQL, points, points.size(), (ps, point) -> {
            ps.setLong(1, runningRecordId);
            ps.setInt(2, point.sequence());
            ps.setObject(3, point.latitude());
            ps.setObject(4, point.longitude());
            ps.setObject(5, point.timestampSeconds());
            ps.setObject(6, point.heartRate());
            ps.setObject(7, point.paceSeconds());
            ps.setObject(8, point.altitude());
            ps.setObject(9, point.accuracy());
            ps.setObject(10, point.cumulativeDistanceMeters());
            ps.setObject(11, point.createdAt());
            ps.setObject(12, point.updatedAt());
        });
    }
}
