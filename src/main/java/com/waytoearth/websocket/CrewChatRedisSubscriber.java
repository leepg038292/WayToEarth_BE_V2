package com.waytoearth.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

/**
 * 다른 인스턴스가 발행한 크루 채팅 메시지를 받아 이 인스턴스에 붙어있는
 * 로컬 세션들에게만 전달한다. 자기 자신이 발행한 메시지는 이미 로컬에 전달
 * 완료된 상태이므로 instanceId로 걸러서 중복 전달을 막는다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CrewChatRedisSubscriber implements MessageListener {

    private final CrewChatWebSocketHandler crewChatWebSocketHandler;
    private final ChatInstanceId chatInstanceId;
    private final ObjectMapper objectMapper;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            CrewChatRelayEnvelope envelope =
                    objectMapper.readValue(message.getBody(), CrewChatRelayEnvelope.class);

            if (chatInstanceId.value().equals(envelope.originInstanceId())) {
                return; // 자기 자신이 발행한 메시지 - 이미 로컬 전달 완료됨
            }

            crewChatWebSocketHandler.broadcastToLocalSessions(
                    envelope.crewId(), envelope.message(), envelope.excludeUserId());

        } catch (Exception e) {
            log.error("크루 채팅 Redis 구독 메시지 처리 실패", e);
        }
    }
}
