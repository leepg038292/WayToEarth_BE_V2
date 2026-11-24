package com.waytoearth.websocket;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;

/**
 * 크루 채팅이 DB를 건드리는 지점(핸드셰이크 시 멤버십/닉네임 조회, 메시지 저장)의
 * 동시 실행 개수를 명시적으로 제한하는 백프레셔.
 *
 * 배경: 가상 스레드를 켜면 Tomcat 스레드 풀이 자연스럽게 해주던 동시성 제한이 없어져서,
 * 채팅 연결이 몰리면 HikariCP 풀·MariaDB 자체가 감당 못 할 만큼의 동시 요청이 그대로
 * DB까지 도달한다. 이 한도를 초과하는 요청은 큐에 쌓아두지 않고 즉시 거절해서,
 * 시스템이 "느려지다 통째로 멈추는" 대신 정해진 부하 수준에서 안정적으로 버티게 한다.
 * (docs/study/04-crew-chat-scalability-redis-pubsub.md §12 참고)
 */
@Component
@Slf4j
public class ChatDbConcurrencyLimiter {

    private final Semaphore semaphore;
    private final Counter rejectedCounter;

    public ChatDbConcurrencyLimiter(
            @Value("${chat.db-concurrency-limit:30}") int limit,
            MeterRegistry meterRegistry) {
        this.semaphore = new Semaphore(limit);
        this.rejectedCounter = Counter.builder("chat_db_concurrency_rejected_total")
                .description("채팅 DB 동시성 한도 초과로 거절된 요청 수")
                .register(meterRegistry);
        log.info("ChatDbConcurrencyLimiter 초기화 - limit={}", limit);
    }

    /**
     * 허가를 즉시 얻을 수 있으면 true를 반환하고 점유한다(대기하지 않음 - 큐잉 없이 fail-fast).
     * 호출자는 반드시 finally 블록에서 {@link #release()}를 호출해야 한다.
     */
    public boolean tryAcquire() {
        boolean acquired = semaphore.tryAcquire();
        if (!acquired) {
            rejectedCounter.increment();
        }
        return acquired;
    }

    public void release() {
        semaphore.release();
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }
}
