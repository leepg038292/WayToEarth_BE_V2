package com.waytoearth.config.async;

import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Method;

/**
 * 러닝 완료 후 부가 로직(크루 통계 등)을 별도 스레드에서 처리하기 위한 비동기 설정.
 * - 스레드풀을 명시적으로 제한(무제한 스레드 생성 방지)
 * - 배포/재시작 시 진행 중인 작업을 최대한 끝내고 종료(그레이스풀 셧다운)
 * - void 비동기 메서드에서 던져진 예외를 반드시 로그로 남김(안 하면 조용히 사라짐)
 */
@Slf4j
@Configuration
@EnableAsync
public class AsyncConfig implements AsyncConfigurer {

    public static final String CREW_STAT_EXECUTOR = "crewStatTaskExecutor";
    public static final String ROUTE_ARCHIVE_EXECUTOR = "routeArchiveTaskExecutor";

    @Bean(name = CREW_STAT_EXECUTOR)
    public ThreadPoolTaskExecutor crewStatTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(200);
        executor.setThreadNamePrefix("crew-stat-async-");

        // 그레이스풀 셧다운: 종료 신호를 받으면 새 작업은 안 받되, 이미 큐에 있거나 실행 중인 작업은
        // 아래 시간만큼 기다렸다가 종료함 (배포/재시작 시 진행 중이던 크루 통계 반영 유실 최소화)
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(20);

        executor.initialize();
        return executor;
    }

    /**
     * 크루 통계와 스레드 풀을 분리 — 러닝 경로 아카이브 복제는 크루 통계보다 부하가 낮고
     * 실패해도 안전망(스윕) 없이 그냥 로그만 남기는 성격이 달라, 이름/큐가 섞이지 않게 별도 executor를 둔다.
     */
    @Bean(name = ROUTE_ARCHIVE_EXECUTOR)
    public ThreadPoolTaskExecutor routeArchiveTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(200);
        executor.setThreadNamePrefix("route-archive-async-");

        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(20);

        executor.initialize();
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (Throwable ex, Method method, Object... params) ->
                log.error("비동기 작업 처리 중 처리되지 않은 예외 발생: method={}, params={}",
                        method.getName(), params, ex);
    }
}
