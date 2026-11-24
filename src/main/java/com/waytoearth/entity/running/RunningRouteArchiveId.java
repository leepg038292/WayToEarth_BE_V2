package com.waytoearth.entity.running;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Embeddable
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class RunningRouteArchiveId implements Serializable {

    @Column(name = "running_record_id")
    private Long runningRecordId;

    @Column(name = "sequence")
    private Integer sequence;
}
