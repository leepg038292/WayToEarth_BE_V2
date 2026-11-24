package com.waytoearth.config.redis;

import com.waytoearth.websocket.CrewChatRedisPublisher;
import com.waytoearth.websocket.CrewChatRedisSubscriber;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * 크루 채팅 인스턴스 간 릴레이용 Redis Pub/Sub 컨테이너.
 * RedisMessageListenerContainer는 내부적으로 자체 스레드로 구독을 유지하고,
 * Lettuce 커넥션이 끊겨도 재연결 시 자동으로 재구독한다.
 */
@Configuration
@RequiredArgsConstructor
public class RedisPubSubConfig {

    private final CrewChatRedisSubscriber crewChatRedisSubscriber;

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory connectionFactory, Executor crewChatRelayTaskExecutor) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        // 기본값(SimpleAsyncTaskExecutor)은 메시지마다 새 스레드를 무제한 생성하므로
        // 반드시 별도의 bounded executor를 지정해야 한다.
        container.setTaskExecutor(crewChatRelayTaskExecutor);
        container.addMessageListener(crewChatRedisSubscriber, new ChannelTopic(CrewChatRedisPublisher.CHANNEL));
        return container;
    }

    @Bean
    public Executor crewChatRelayTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("crew-chat-relay-");
        executor.initialize();
        return executor;
    }
}
