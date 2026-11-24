package com.waytoearth.websocket;

import com.waytoearth.dto.websocket.ChatMessage;

/**
 * 인스턴스 간 채팅 메시지 릴레이용 Redis Pub/Sub 페이로드.
 * originInstanceId로 발행한 인스턴스 자신이 재구독해서 중복 처리하는 것을 방지한다.
 */
public record CrewChatRelayEnvelope(
        String originInstanceId,
        Long crewId,
        Long excludeUserId,
        ChatMessage message
) {
}
