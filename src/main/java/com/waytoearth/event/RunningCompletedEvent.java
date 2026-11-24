package com.waytoearth.event;

/**
 * 러닝 완료(핵심 저장 커밋) 후 부가 로직(크루 통계 등)을 트리거하기 위한 이벤트.
 * outboxId만 담아서, 리스너는 그 outbox row를 찾아 처리한다(중복 처리 방지는 outbox 쪽에서 보장).
 */
public record RunningCompletedEvent(Long outboxId) {
}
