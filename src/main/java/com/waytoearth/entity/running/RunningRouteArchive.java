package com.waytoearth.entity.running;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * running_route의 완료된 러닝 좌표를 (running_record_id, sequence) 복합키로 복제해둔 읽기 전용 아카이브.
 * 클러스터드 인덱스가 복합키라 한 러닝의 좌표가 물리적으로 뭉쳐 저장되어, IDENTITY PK인 running_route보다
 * 조회가 빠름(docs/study/01-running-route-pk-innodb-internals.md 20절 벤치마크 참고).
 * 쓰기는 이 엔티티의 JPA persist가 아니라 RunningRouteArchiveWriter의 JdbcTemplate 배치로 이루어지므로
 * BaseTimeEntity(@CreatedDate/@LastModifiedDate)를 상속하지 않고 평범한 컬럼으로 매핑한다.
 */
@Entity
@Table(name = "running_route_archive")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RunningRouteArchive {

    @EmbeddedId
    private RunningRouteArchiveId id;

    @Column(name = "latitude", nullable = false)
    private Double latitude;

    @Column(name = "longitude", nullable = false)
    private Double longitude;

    @Column(name = "timestamp_seconds")
    private Integer timestampSeconds;

    @Column(name = "heart_rate")
    private Integer heartRate;

    @Column(name = "pace_seconds")
    private Integer paceSeconds;

    @Column(name = "altitude")
    private Double altitude;

    @Column(name = "accuracy")
    private Double accuracy;

    @Column(name = "cumulative_distance_meters")
    private Integer cumulativeDistanceMeters;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
