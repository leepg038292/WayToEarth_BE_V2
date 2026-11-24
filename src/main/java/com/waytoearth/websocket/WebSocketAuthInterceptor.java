package com.waytoearth.websocket;

import com.waytoearth.entity.user.User;
import com.waytoearth.repository.crew.CrewMemberRepository;
import com.waytoearth.repository.user.UserRepository;
import com.waytoearth.service.auth.JwtTokenProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class WebSocketAuthInterceptor implements HandshakeInterceptor {

    private static final String UNKNOWN_NICKNAME = "Unknown";

    private final JwtTokenProvider jwtTokenProvider;
    private final CrewMemberRepository crewMemberRepository;
    private final UserRepository userRepository;
    private final ChatDbConcurrencyLimiter concurrencyLimiter;
    private final ChatObservability obs;

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {

        try {
            String token = extractTokenFromRequest(request);

            if (token == null || !jwtTokenProvider.validateToken(token)) {
                log.warn("WebSocket 연결 거부 - 유효하지 않은 토큰: {}", request.getURI());
                return false;
            }

            Long userId = jwtTokenProvider.getUserIdFromToken(token);

            // URL에서 crewId 추출
            Long crewId = extractCrewIdFromPath(request.getURI().getPath());

            if (crewId == null) {
                log.warn("WebSocket 연결 거부 - crewId를 찾을 수 없음: {}", request.getURI());
                return false;
            }

            // 여기서부터 DB를 건드리는 구간(멤버십 확인 + 닉네임 조회) - 백프레셔로 동시
            // 실행 개수를 제한. 허가를 못 받으면 큐에 쌓지 않고 즉시 핸드셰이크를 거절한다
            // (docs/study/04-crew-chat-scalability-redis-pubsub.md §12 참고)
            if (!concurrencyLimiter.tryAcquire()) {
                obs.count("chat_handshake_total", "result", "rejected_semaphore");
                log.warn("WebSocket 연결 거부 - DB 동시성 한도 초과: userId={}, crewId={}", userId, crewId);
                return false;
            }

            String nickname;
            try {
                // 크루 멤버인지 확인
                obs.count("chat_membership_query_total", "site", "handshake");
                if (!crewMemberRepository.isUserMemberOfCrew(userId, crewId)) {
                    obs.count("chat_handshake_total", "result", "rejected_not_member");
                    log.warn("WebSocket 연결 거부 - 크루 멤버 아님: userId={}, crewId={}", userId, crewId);
                    return false;
                }

                // 닉네임을 핸드셰이크 시점에 한 번만 조회해 세션에 캐싱 - 입장/메시지/퇴장마다
                // 매번 DB를 다시 때리지 않기 위함 (메시지 전송 시점에 반복 조회하던 게 실측으로
                // 확인된 병목이었음)
                obs.count("chat_user_query_total", "site", "handshake_nickname");
                User user = userRepository.findById(userId).orElse(null);
                nickname = user != null ? user.getNickname() : UNKNOWN_NICKNAME;
            } finally {
                concurrencyLimiter.release();
            }

            attributes.put("userId", userId);
            attributes.put("crewId", crewId);
            attributes.put("token", token);
            attributes.put("nickname", nickname);

            obs.count("chat_handshake_total", "result", "accepted");
            log.info("WebSocket 인증 성공 - userId: {}, crewId: {}", userId, crewId);
            return true;

        } catch (Exception e) {
            log.error("WebSocket 인증 실패", e);
            return false;
        }
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                              WebSocketHandler wsHandler, Exception exception) {
        if (exception != null) {
            log.error("WebSocket handshake 실패", exception);
        }
    }

    private String extractTokenFromRequest(ServerHttpRequest request) {
        // 보안상 Authorization 헤더만 사용 (URL 쿼리 파라미터 사용 금지)
        String authHeader = request.getHeaders().getFirst("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            return authHeader.substring(7);
        }

        // SEC-INFO 헤더에서도 토큰 확인 (SockJS fallback)
        String secInfoHeader = request.getHeaders().getFirst("Sec-WebSocket-Protocol");
        if (secInfoHeader != null && secInfoHeader.startsWith("Bearer.")) {
            return secInfoHeader.substring(7); // "Bearer." 제거
        }

        log.warn("WebSocket 연결에서 Authorization 헤더를 찾을 수 없음: {}",
                request.getURI().getPath());
        return null;
    }

    /**
     * WebSocket URL에서 crewId 추출
     * 예: /ws/crew/123/chat → 123
     */
    private Long extractCrewIdFromPath(String path) {
        try {
            String[] parts = path.split("/");
            for (int i = 0; i < parts.length - 1; i++) {
                if ("crew".equals(parts[i]) && i + 1 < parts.length) {
                    return Long.parseLong(parts[i + 1]);
                }
            }
        } catch (NumberFormatException e) {
            log.error("crewId 파싱 실패: {}", path, e);
        }
        return null;
    }
}