package com.waytoearth.repository.crew;

import com.waytoearth.entity.crew.CrewEntity;
import com.waytoearth.entity.user.User;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import jakarta.persistence.LockModeType;

import java.util.List;
import java.util.Optional;

@Repository
public interface CrewRepository extends JpaRepository<CrewEntity, Long>, CrewRepositoryCustom {

    List<CrewEntity> findByOwner(User owner);

    @Query("SELECT c FROM CrewEntity c " +
           "JOIN FETCH c.owner " +
           "WHERE c.id = :crewId")
    Optional<CrewEntity> findByIdWithOwner(@Param("crewId") Long crewId);

    //가입 가능한 크루 조회
    @Query("SELECT c FROM CrewEntity c " +
           "WHERE c.id NOT IN (" +
           "  SELECT cm.crew.id FROM CrewMemberEntity cm " +
           "  WHERE cm.user = :user" +
           ") " +
           "AND c.id NOT IN (" +
           "  SELECT jr.crew.id FROM CrewJoinRequestEntity jr " +
           "  WHERE jr.user = :user AND jr.status = 'PENDING'" +
           ") " +
           "ORDER BY c.createdAt DESC")
    List<CrewEntity> findJoinableCrews(@Param("user") User user);

    /**
     * 페이징 지원 메서드들
     */
    @Query("SELECT c FROM CrewEntity c " +
           "JOIN FETCH c.owner " +
           "ORDER BY c.createdAt DESC")
    Page<CrewEntity> findAllWithOwner(Pageable pageable);

    //페이징 지원 크루 검색 - N+1 방지
    @Query("SELECT c FROM CrewEntity c " +
           "JOIN FETCH c.owner " +
           "WHERE c.name LIKE %:keyword%")
    Page<CrewEntity> findByNameContainingWithOwner(@Param("keyword") String keyword, Pageable pageable);

    //페이징 지원 사용자 크루 조회 - N+1 방지
    @Query("SELECT DISTINCT c FROM CrewEntity c " +
           "JOIN FETCH c.owner " +
           "JOIN c.members m " +
           "WHERE m.user = :user")
    Page<CrewEntity> findCrewsByUserWithOwnerPaged(@Param("user") User user, Pageable pageable);

    // Concurrency control: lock crew row for updates within a transaction
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT c FROM CrewEntity c WHERE c.id = :crewId")
    Optional<CrewEntity> findByIdForUpdate(@Param("crewId") Long crewId);

    // 크루 현재 멤버 수 원자적 증가/감소 (낙관적 락+엔티티 mutate 대신 사용 —
    // 여러 멤버가 동시에 가입/탈퇴/추방될 때 재시도 없이 안전하게 처리하기 위함)
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE CrewEntity c SET c.currentMembers = c.currentMembers + :delta " +
           "WHERE c.id = :crewId AND c.currentMembers + :delta >= 0 AND c.currentMembers + :delta <= c.maxMembers")
    int updateCurrentMembersAtomically(@Param("crewId") Long crewId, @Param("delta") int delta);
}
