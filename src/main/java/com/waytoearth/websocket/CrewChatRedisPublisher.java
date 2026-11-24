package com.waytoearth.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.waytoearth.dto.websocket.ChatMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CrewChatRedisPublisher {

    public static final String CHANNEL = "crew-chat:broadcast";

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;
    private final ChatInstanceId chatInstanceId;

    public void publish(Long crewId, ChatMessage message, Long excludeUserId) {
        try {
            CrewChatRelayEnvelope envelope =
                    new CrewChatRelayEnvelope(chatInstanceId.value(), crewId, excludeUserId, message);
            stringRedisTemplate.convertAndSend(CHANNEL, objectMapper.writeValueAsString(envelope));
        } catch (Exception e) {
            log.error("크루 채팅 Redis 발행 실패 - crewId: {}", crewId, e);
        }
    }
}
