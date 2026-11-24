package com.waytoearth.repository.running;

import com.waytoearth.entity.running.RunningRoute;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface RunningRouteRepository extends JpaRepository<RunningRoute, Long> {

    /**
     * running_route_archive 도입 이전에 완료됐거나 아카이브 쓰기가 실패한 레거시 기록을 위한 폴백 조회.
     */
    List<RunningRoute> findByRunningRecord_IdOrderBySequenceAsc(Long recordId);
}