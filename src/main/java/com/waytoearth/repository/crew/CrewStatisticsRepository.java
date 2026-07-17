package com.waytoearth.repository.crew;

import com.waytoearth.entity.crew.CrewEntity;
import com.waytoearth.entity.crew.CrewStatisticsEntity;
import com.waytoearth.entity.user.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

import java.util.List;
import java.util.Optional;

@Repository
public interface CrewStatisticsRepository extends JpaRepository<CrewStatisticsEntity, Long>, CrewStatisticsRepositoryCustom {

    Optional<CrewStatisticsEntity> findByCrewAndMonth(CrewEntity crew, String month);

    List<CrewStatisticsEntity> findByCrewOrderByMonthDesc(CrewEntity crew);

    List<CrewStatisticsEntity> findByCrewAndMonthBetweenOrderByMonth(
            CrewEntity crew, String startMonth, String endMonth);

    //MVP 사용자 포함 조회
    @Query("SELECT cs FROM CrewStatisticsEntity cs " +
           "LEFT JOIN FETCH cs.mvpUser " +
           "WHERE cs.crew = :crew AND cs.month = :month")
    Optional<CrewStatisticsEntity> findByCrewAndMonthWithMvp(@Param("crew") CrewEntity crew,
                                                            @Param("month") String month);

    //기본적인 존재 여부 확인
    boolean existsByCrewAndMonth(CrewEntity crew, String month);

    /**
     * 러닝횟수·총거리·활성멤버·평균페이스를 전부 SQL 한 문장으로 원자적으로 갱신.
     *
     * 주의(실측으로 확인한 MariaDB 동작): UPDATE의 SET절은 왼쪽에서 오른쪽으로 "순차 평가"됨 —
     * 즉 total_distance를 먼저 갱신해버리면, 그 뒤에 나오는 avg_pace_seconds 계산식은
     * "갱신 전 값"이 아니라 "이미 새로 갱신된 값"을 보게 됨(표준 SQL의 일반적 기대와 다름, 실제로
     * 잘못된 값이 나오는 걸 검증 테스트에서 확인함). 그래서 avg_pace_seconds 계산을
     * total_distance 갱신보다 반드시 "먼저" 둬서, 계산식이 갱신 전 total_distance를 보게 함.
     * 네이티브 쿼리 사용: JPQL은 SET절에서 다른 컬럼을 참조하는 산술식(CASE 안 나눗셈 등)을 지원 안 함.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE crew_statistics SET " +
           "avg_pace_seconds = CASE " +
           "    WHEN avg_pace_seconds IS NULL OR total_distance <= 0 THEN :memberPaceSeconds " +
           "    ELSE (avg_pace_seconds * total_distance + :memberPaceSeconds * :distance) / (total_distance + :distance) " +
           "END, " +
           "run_count = run_count + :runCount, " +
           "total_distance = total_distance + :distance, " +
           "active_members = CASE WHEN :isNewActiveMember = true THEN active_members + 1 ELSE active_members END, " +
           "updated_at = NOW(6) " +
           "WHERE crew_id = :crewId AND month = :month",
           nativeQuery = true)
    int updateStatisticsAndPaceAtomically(@Param("crewId") Long crewId, @Param("month") String month,
                                          @Param("runCount") int runCount, @Param("distance") BigDecimal distance,
                                          @Param("memberPaceSeconds") BigDecimal memberPaceSeconds,
                                          @Param("isNewActiveMember") boolean isNewActiveMember);

    //월간 MVP 업데이트 (원자적 — JPA 엔티티 mutate 대신 직접 UPDATE)
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE CrewStatisticsEntity cs SET cs.mvpUser = :mvpUser, cs.mvpDistance = :mvpDistance " +
           "WHERE cs.crew.id = :crewId AND cs.month = :month")
    int updateMvpUser(@Param("crewId") Long crewId, @Param("month") String month,
                      @Param("mvpUser") User mvpUser, @Param("mvpDistance") BigDecimal mvpDistance);

    //회원 탈퇴 시 MVP 참조 해제
    @Modifying
    @Query("UPDATE CrewStatisticsEntity cs SET cs.mvpUser = null, cs.mvpDistance = null " +
           "WHERE cs.mvpUser.id = :userId")
    int clearMvpByUserId(@Param("userId") Long userId);

    //크루 현재 멤버 수 원자적 증가/감소
    @Modifying
    @Query("UPDATE CrewEntity c SET c.currentMembers = c.currentMembers + :delta " +
           "WHERE c.id = :crewId AND c.currentMembers + :delta >= 0 AND c.currentMembers + :delta <= c.maxMembers")
    int updateCurrentMembersAtomically(@Param("crewId") Long crewId, @Param("delta") int delta);

    //크루 삭제 시 모든 통계 삭제
    @Modifying
    @Query("DELETE FROM CrewStatisticsEntity cs WHERE cs.crew.id = :crewId")
    void deleteAllByCrewId(@Param("crewId") Long crewId);
}