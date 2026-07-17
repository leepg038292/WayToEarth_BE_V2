package com.waytoearth.repository.running;

import com.waytoearth.entity.running.RunningRouteArchive;
import com.waytoearth.entity.running.RunningRouteArchiveId;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface RunningRouteArchiveRepository extends JpaRepository<RunningRouteArchive, RunningRouteArchiveId> {

    List<RunningRouteArchive> findById_RunningRecordIdOrderById_SequenceAsc(Long runningRecordId);
}
