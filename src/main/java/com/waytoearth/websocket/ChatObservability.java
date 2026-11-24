package com.waytoearth.websocket;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * 크루 채팅 경로의 관측 장치 (loadtest/harness/HARNESS.md 참고).
 *
 * 1) 카운터 - 항상 켜짐. 코드 경로별 "몇 번 일어났나"를 센다 (Micrometer → /actuator/prometheus).
 * 2) 플라이트 레코더 - chat.trace.enabled=true일 때만. 이벤트 하나하나를 링 버퍼에 남기고
 *    /actuator/chattrace 로 덤프한다. 집계값만으로는 "왜 그 숫자인지"를 못 밝히는 경우를 위한 것
 *    (토스 Redis Tail Latency 사례의 Go execution tracer + flight recorder 패턴을 차용:
 *    상시 기록하되 메모리 상한을 두고, 필요한 순간에만 꺼낸다).
 *
 * 로그 대신 메모리에 남기는 이유: 이 경로의 문제 자체가 "로그 쓰기 락 경합"이었기 때문에
 * (docs/study/04 §15.1) 관측 장치가 같은 락을 쓰면 측정 대상을 오염시킨다.
 */
@Component
@Endpoint(id = "chattrace")
public class ChatObservability {

    private final MeterRegistry registry;
    private final boolean traceEnabled;
    private final AtomicReferenceArray<String> ring;
    private final int capacity;
    private final AtomicLong seq = new AtomicLong();
    private final AtomicLong broadcastSeq = new AtomicLong();
    private final long epochBaseMs = System.currentTimeMillis();
    private final long nanoBase = System.nanoTime();
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Environment environment;

    // 덤프에 "실제로 적용된" 설정값을 같이 싣는다 - 실험 카드 값이 진짜 들어갔는지 검증용 (HARNESS.md R11)
    private static final String[] EFFECTIVE_PROPS = {
            "chat.rate-limit-before-db", "chat.membership-mode", "chat.db-concurrency-limit", "chat.trace.capacity",
            "spring.threads.virtual.enabled", "spring.datasource.hikari.maximum-pool-size",
            "spring.datasource.hikari.minimum-idle", "server.tomcat.threads.max", "logging.level.com.waytoearth"
    };

    public ChatObservability(MeterRegistry registry, Environment environment,
                             @Value("${chat.trace.enabled:false}") boolean traceEnabled,
                             @Value("${chat.trace.capacity:500000}") int capacity) {
        this.registry = registry;
        this.environment = environment;
        this.traceEnabled = traceEnabled;
        this.capacity = capacity;
        this.ring = new AtomicReferenceArray<>(traceEnabled ? capacity : 1);
    }

    // ---------------------------------------------------------------- counters

    /** name + 태그 쌍(key, value, key, value...)으로 카운터 1 증가 */
    public void count(String name, String... tags) {
        counter(name, tags).increment();
    }

    public void count(String name, double amount, String... tags) {
        counter(name, tags).increment(amount);
    }

    private Counter counter(String name, String... tags) {
        String key = name + "|" + String.join(",", tags);
        return counters.computeIfAbsent(key, k -> Counter.builder(name).tags(tags).register(registry));
    }

    // ---------------------------------------------------------------- flight recorder

    public boolean traceEnabled() {
        return traceEnabled;
    }

    public long nextBroadcastId() {
        return broadcastSeq.incrementAndGet();
    }

    /** 현재 시각(ms, 소수점 3자리) - 실행 내 모든 이벤트가 같은 기준을 쓰도록 단조 시계 기반 */
    public double nowMs() {
        return epochBaseMs + (System.nanoTime() - nanoBase) / 1_000_000.0;
    }

    /**
     * 이벤트 1건 기록. 형식: seq \t epochMs \t thread \t type \t key=value ...
     * 문자열 하나로 만들어 넣는 이유: 덤프·파싱이 단순하고 이벤트 타입마다 필드가 달라도 됨.
     */
    public void trace(String type, Object... kv) {
        if (!traceEnabled) {
            return;
        }
        long n = seq.getAndIncrement();
        StringBuilder sb = new StringBuilder(96);
        sb.append(n).append('\t').append(String.format("%.3f", nowMs())).append('\t')
          .append(Thread.currentThread().getName()).append('\t').append(type);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            sb.append('\t').append(kv[i]).append('=').append(kv[i + 1]);
        }
        ring.set((int) (n % capacity), sb.toString());
    }

    @ReadOperation
    public Map<String, Object> dump() {
        long total = seq.get();
        long start = Math.max(0, total - capacity);
        List<String> events = new ArrayList<>((int) Math.min(total, capacity));
        for (long i = start; i < total; i++) {
            String e = ring.get((int) (i % capacity));
            if (e != null) {
                events.add(e);
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, String> effective = new LinkedHashMap<>();
        for (String key : EFFECTIVE_PROPS) {
            effective.put(key, environment.getProperty(key, "(unset → 코드/프레임워크 기본값)"));
        }
        out.put("effectiveConfig", effective);
        out.put("enabled", traceEnabled);
        out.put("capacity", capacity);
        out.put("recorded", total);
        out.put("dropped", start); // 링 버퍼가 덮어써서 잃은 이벤트 수 - 0이 아니면 분석 결과에 명시해야 함
        out.put("events", events);
        return out;
    }

    @DeleteOperation
    public Map<String, Object> reset() {
        for (int i = 0; i < ring.length(); i++) {
            ring.set(i, null);
        }
        seq.set(0);
        broadcastSeq.set(0);
        return Map.of("reset", true);
    }
}
