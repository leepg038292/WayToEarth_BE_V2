package com.waytoearth.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.waytoearth.dto.websocket.ChatMessage;
import com.waytoearth.entity.crew.CrewChatEntity;
import com.waytoearth.repository.crew.CrewMemberRepository;
import com.waytoearth.repository.crew.CrewRepository;
import com.waytoearth.service.crew.CrewChatService;
import com.waytoearth.util.MessageSanitizer;
import com.waytoearth.util.ChatRateLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class CrewChatWebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper objectMapper;
    private final CrewChatService crewChatService;
    private final CrewRepository crewRepository;
    private final CrewMemberRepository crewMemberRepository;
    private final MessageSanitizer messageSanitizer;
    private final ChatRateLimiter chatRateLimiter;
    private final WebSocketSessionManager sessionManager;
    private final CrewChatRedisPublisher crewChatRedisPublisher;
    private final ChatDbConcurrencyLimiter concurrencyLimiter;
    private final ChatObservability obs;

    // true면 레이트리미터를 세마포어·DB 조회보다 먼저 검사한다. 거절될 메시지가 DB 커넥션·세마포어를
    // 쓰지 않게 하기 위함. 기존 순서(false)와 실측 비교 중 (docs/study/05-part4-redefinition-toss.md §3 F2, §6 E2)
    @org.springframework.beans.factory.annotation.Value("${chat.rate-limit-before-db:false}")
    private boolean rateLimitBeforeDb;

    // 실험용 토글(J1·P1 해명 카드 Y4): per_broadcast = 결정 D-2(F9) 이후 현재 동작(기본값),
    // per_recipient = F9 이전 동작(수신자마다 isUserMemberOfCrew 1회). 운영 기본값은 바꾸지 않는다.
    @org.springframework.beans.factory.annotation.Value("${chat.membership-mode:per_broadcast}")
    private String membershipMode;

    // 결정 D-1(F8): 표준 WebSocket 세션(JSR-356)은 동시 전송을 허용하지 않는다(Spring Reference "WebSocket API").
    // 세션마다 ConcurrentWebSocketSessionDecorator를 하나만 만들고, 이 세션으로 나가는 모든 전송(브로드캐스트·오류 응답)이
    // 이것을 거치게 한다. 한도 값은 Spring이 STOMP 설정에서 같은 데코레이터에 쓰는 기본값(전송 10초, 버퍼 512K).
    private static final int SEND_TIME_LIMIT_MS = 10_000;
    private static final int SEND_BUFFER_LIMIT_BYTES = 512 * 1024;
    private final Map<String, WebSocketSession> outbound = new ConcurrentHashMap<>();

    private WebSocketSession outboundOf(WebSocketSession raw) {
        return outbound.getOrDefault(raw.getId(), raw);
    }

    /** 결정 D-1(F8): 맵에서 지우는 세션은 반드시 닫는다 - 안 닫으면 클라이언트는 연결된 줄 알고 메시지를 영영 못 받는다(조용한 유실) */
    private void closeRemoved(WebSocketSession s, String reason) {
        try {
            if (s.isOpen()) {
                s.close(CloseStatus.SESSION_NOT_RELIABLE);
                obs.count("chat_session_force_closed_total", "reason", reason);
            }
        } catch (Exception ignored) {
            // 이미 닫히는 중
        }
    }

    // 크루별 연결된 세션들을 관리 (crewId -> Map<userId, WebSocketSession>)
    private final Map<Long, Map<Long, WebSocketSession>> crewConnections = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        String uri = session.getUri().toString();
        Long crewId = extractCrewIdFromUri(uri);
        Long userId = extractUserIdFromSession(session);

        if (crewId == null || userId == null) {
            session.close(CloseStatus.BAD_DATA.withReason("Invalid crew ID or user ID"));
            return;
        }

        // 크루 멤버십 확인
        obs.count("chat_membership_query_total", "site", "connect");
        if (!crewMemberRepository.isUserMemberOfCrew(userId, crewId)) {
            session.close(CloseStatus.NOT_ACCEPTABLE.withReason("Not a crew member"));
            return;
        }

        // 세션 저장
        Map<Long, WebSocketSession> sessionsOfCrew = crewConnections.computeIfAbsent(crewId, k -> new ConcurrentHashMap<>());
        WebSocketSession decorated = new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, SEND_BUFFER_LIMIT_BYTES);
        outbound.put(session.getId(), decorated);
        WebSocketSession previous = sessionsOfCrew.put(userId, decorated);
        obs.count("chat_session_opened_total");
        obs.trace("OPEN", "crew", crewId, "user", userId, "sid", session.getId(),
                "map", sessionsOfCrew.size(), "replaced", previous != null);

        // 세션 활동 시간 기록
        sessionManager.updateSessionActivity(session);

        log.info("웹소켓 연결 성공 - crewId: {}, userId: {}", crewId, userId);

        // 연결 알림 메시지 전송 (시스템 메시지) - 닉네임은 핸드셰이크 시점에 캐싱된 걸 재사용
        String nickname = extractNicknameFromSession(session);
        ChatMessage joinMessage = ChatMessage.builder()
                .crewId(crewId)
                .senderId(userId)
                .senderName(nickname)
                .message(nickname + "님이 채팅방에 참여했습니다.")
                .messageType(CrewChatEntity.MessageType.SYSTEM)
                .timestamp(java.time.Instant.now())
                .build();

        publishAndBroadcast(crewId, joinMessage, userId, "join");
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            String uri = session.getUri().toString();
            Long crewId = extractCrewIdFromUri(uri);
            Long userId = extractUserIdFromSession(session);

            if (crewId == null || userId == null) {
                return;
            }

            // 세션 활동 시간 업데이트
            sessionManager.updateSessionActivity(session);

            ChatMessage chatMessage = objectMapper.readValue(message.getPayload(), ChatMessage.class);

            // 여기서부터 DB를 건드리는 구간(멤버십 재확인 + 메시지 저장) - 백프레셔로 동시
            // 실행 개수를 제한. 허가를 못 받으면 큐에 쌓지 않고 즉시 거절한다
            // (docs/study/04-crew-chat-scalability-redis-pubsub.md §12 참고)
            if (rateLimitBeforeDb && !chatRateLimiter.canSendMessage(userId)) {
                sendErrorMessage(session, "메시지 전송 속도가 너무 빠릅니다. 잠시 후 다시 시도해주세요.");
                return;
            }

            if (!concurrencyLimiter.tryAcquire()) {
                sendErrorMessage(session, "지금 서버가 바빠요. 잠시 후 다시 시도해주세요.");
                return;
            }

            CrewChatEntity savedMessage;
            try {
                // Authorization re-check at send time
                obs.count("chat_membership_query_total", "site", "send");
                if (!crewMemberRepository.isUserMemberOfCrew(userId, crewId)) {
                    sendErrorMessage(session, "권한이 없습니다. 크루 멤버가 아닙니다.");
                    session.close(CloseStatus.POLICY_VIOLATION);
                    return;
                }

                if (chatMessage.getCrewId() != null && !crewId.equals(chatMessage.getCrewId())) {
                    sendErrorMessage(session, "잘못된 대상 크루입니다.");
                    return;
                }

                // Rate Limiting 검증 (이제 검증과 기록이 원자적으로 처리됨)
                if (!rateLimitBeforeDb && !chatRateLimiter.canSendMessage(userId)) {
                    sendErrorMessage(session, "메시지 전송 속도가 너무 빠릅니다. 잠시 후 다시 시도해주세요.");
                    return;
                }

                // 메시지 유효성 검증
                if (chatMessage.getMessage() == null || chatMessage.getMessage().trim().isEmpty()) {
                    sendErrorMessage(session, "메시지 내용이 비어있습니다.");
                    return;
                }

                // 메시지 길이 검증
                if (!messageSanitizer.isValidLength(chatMessage.getMessage())) {
                    sendErrorMessage(session, "메시지는 1자 이상 1000자 이하여야 합니다.");
                    return;
                }

                // 스팸 메시지 검증
                if (messageSanitizer.isSpamMessage(chatMessage.getMessage())) {
                    sendErrorMessage(session, "스팸성 메시지는 전송할 수 없습니다.");
                    return;
                }

                // 메시지 내용 정제 (XSS 방지)
                String sanitizedMessage = messageSanitizer.sanitizeMessage(chatMessage.getMessage());

                // 메시지 저장 (정제된 메시지 사용)
                savedMessage = crewChatService.saveMessage(crewId, userId,
                        sanitizedMessage, chatMessage.getMessageType());

                // Rate Limiting 기록은 이미 canSendMessage에서 처리됨
            } finally {
                concurrencyLimiter.release();
            }

            // 응답 메시지 구성 - 닉네임은 핸드셰이크 시점에 캐싱된 걸 재사용(매 메시지마다 DB 재조회 금지)
            ChatMessage responseMessage = ChatMessage.builder()
                    .messageId(savedMessage.getId())
                    .crewId(crewId)
                    .senderId(userId)
                    .senderName(extractNicknameFromSession(session))
                    .message(savedMessage.getMessage())
                    .messageType(savedMessage.getMessageType())
                    .timestamp(savedMessage.getSentAt().atZone(java.time.ZoneId.systemDefault()).toInstant())
                    .build();

            // 크루 멤버들에게 브로드캐스트 (로컬 세션 + Redis Pub/Sub으로 다른 인스턴스에도 릴레이)
            obs.count("chat_message_saved_total");
            publishAndBroadcast(crewId, responseMessage, null, "message");

            log.debug("메시지 전송 완료 - crewId: {}, userId: {}, messageId: {}",
                     crewId, userId, savedMessage.getId());

        } catch (Exception e) {
            log.error("메시지 처리 중 오류 발생", e);
            sendErrorMessage(session, "메시지 처리 중 오류가 발생했습니다.");
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        String uri = session.getUri().toString();
        Long crewId = extractCrewIdFromUri(uri);
        Long userId = extractUserIdFromSession(session);

        if (crewId != null && userId != null) {
            Map<Long, WebSocketSession> crewSessions = crewConnections.get(crewId);
            boolean wasInMap = false;
            if (crewSessions != null) {
                WebSocketSession removedSession = crewSessions.remove(userId); // 기존 동작 유지(userId 기준 제거)
                wasInMap = removedSession == outboundOf(session);
                if (crewSessions.isEmpty()) {
                    crewConnections.remove(crewId);
                }
            }
            outbound.remove(session.getId());
            obs.count("chat_session_closed_total", "was_in_map", String.valueOf(wasInMap));
            obs.trace("CLOSE", "crew", crewId, "user", userId, "sid", session.getId(),
                    "code", status.getCode(), "wasInMap", wasInMap,
                    "map", crewSessions == null ? 0 : crewSessions.size());

            // 세션 매니저에서 제거
            sessionManager.removeSession(session);

            log.info("웹소켓 연결 종료 - crewId: {}, userId: {}", crewId, userId);

            // 퇴장 알림 메시지 전송 (시스템 메시지) - 닉네임은 핸드셰이크 시점에 캐싱된 걸 재사용
            String nickname = extractNicknameFromSession(session);
            ChatMessage leaveMessage = ChatMessage.builder()
                    .crewId(crewId)
                    .senderId(userId)
                    .senderName(nickname)
                    .message(nickname + "님이 채팅방을 나갔습니다.")
                    .messageType(CrewChatEntity.MessageType.SYSTEM)
                    .timestamp(java.time.Instant.now())
                    .build();

            publishAndBroadcast(crewId, leaveMessage, userId, "leave");
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("웹소켓 전송 오류", exception);
        session.close(CloseStatus.SERVER_ERROR);
    }

    /**
     * 로컬 세션 전달 + Redis Pub/Sub 발행을 함께 수행한다.
     * ALB가 크루원을 서로 다른 인스턴스에 배치하는 경우, 로컬 전달만으로는
     * 해당 인스턴스에 붙어있는 세션에만 도달하므로 다른 인스턴스에도 릴레이해야 한다.
     */
    private void publishAndBroadcast(Long crewId, ChatMessage message, Long excludeUserId, String kind) {
        broadcastToLocalSessions(crewId, message, excludeUserId, kind);
        crewChatRedisPublisher.publish(crewId, message, excludeUserId);
    }

    /**
     * 이 인스턴스에 붙어있는 로컬 세션들에게만 전달한다.
     * 로컬에서 발신된 메시지(publishAndBroadcast)와, 다른 인스턴스가 Redis로 릴레이한
     * 메시지(CrewChatRedisSubscriber) 양쪽에서 재사용된다.
     */
    void broadcastToLocalSessions(Long crewId, ChatMessage message, Long excludeUserId) {
        broadcastToLocalSessions(crewId, message, excludeUserId, "relay");
    }

    void broadcastToLocalSessions(Long crewId, ChatMessage message, Long excludeUserId, String kind) {
        Map<Long, WebSocketSession> crewSessions = crewConnections.get(crewId);
        if (crewSessions == null || crewSessions.isEmpty()) {
            return;
        }
        // 관측: 이 브로드캐스트 1회가 몇 명에게, 몇 번 DB를 묻고, 몇 번 실패했는지 (HARNESS.md "브로드캐스트 장부")
        final long bid = obs.nextBroadcastId();
        final double startMs = obs.nowMs();
        final int mapAtStart = crewSessions.size();
        final int[] tally = new int[5]; // 0=considered 1=membershipQuery 2=sendOk 3=sendFail 4=removedOther
        obs.count("chat_broadcast_total", "kind", kind);
        // 계측(J1·P1): 브로드캐스트 안에서 멤버십 조회 호출(커넥션 획득 포함)과 전송에 쓴 시간 누적 [0]=조회 ns [1]=전송 ns
        final long[] spentNs = new long[2];
        final boolean perRecipient = "per_recipient".equals(membershipMode);
        final java.util.Set<Long> memberIds;
        if (perRecipient) {
            memberIds = null;
        } else {
            // 결정 D-2(F9): 수신자별 멤버십 조회(N회) 대신 브로드캐스트당 1회 조회 후 메모리에서 확인
            long q0 = System.nanoTime();
            memberIds = new java.util.HashSet<>(crewMemberRepository.findMemberUserIds(crewId));
            spentNs[0] += System.nanoTime() - q0;
            tally[1]++; // DB 조회 1회
        }

        String messageJson;
        try {
            messageJson = objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            log.error("메시지 JSON 변환 실패", e);
            return;
        }

        crewSessions.entrySet().removeIf(entry -> {
            Long userId = entry.getKey();
            WebSocketSession session = entry.getValue();

            // 제외할 사용자는 스킵
            if (excludeUserId != null && excludeUserId.equals(userId)) {
                return false;
            }
            tally[0]++;
            // 계측(P1): 100명마다 진행 시점 기록 → 브로드캐스트 내부 진행 속도를 1초 미만 해상도로
            if (tally[0] % 100 == 0) {
                obs.trace("PROG", "bid", bid, "n", tally[0], "mq", tally[1], "qMs", String.format("%.3f", spentNs[0] / 1e6));
            }

            try {
                // 권한 재확인: 수신 대상의 멤버십 상태 확인 (per_broadcast: 시작 시 1회 조회한 집합 / per_recipient: 매번 DB)
                final boolean member;
                if (perRecipient) {
                    long q0 = System.nanoTime();
                    member = crewMemberRepository.isUserMemberOfCrew(userId, crewId);
                    spentNs[0] += System.nanoTime() - q0;
                    tally[1]++;
                } else {
                    member = memberIds.contains(userId);
                }
                if (!member) {
                    sessionManager.removeSession(session);
                    closeRemoved(session, "not_member");
                    tally[4]++;
                    obs.count("chat_session_removed_total", "reason", "not_member");
                    obs.trace("REMOVE", "bid", bid, "user", userId, "sid", session.getId(), "reason", "not_member");
                    return true;
                }

                if (session.isOpen() && !sessionManager.isSessionExpired(session)) {
                    long s0 = System.nanoTime();
                    session.sendMessage(new TextMessage(messageJson));
                    spentNs[1] += System.nanoTime() - s0;
                    tally[2]++;
                    return false;
                } else {
                    // 닫힌 세션이거나 타임아웃된 세션은 제거
                    sessionManager.removeSession(session);
                    tally[4]++;
                    String reason = session.isOpen() ? "expired" : "closed";
                    closeRemoved(session, reason);
                    obs.count("chat_session_removed_total", "reason", reason);
                    obs.trace("REMOVE", "bid", bid, "user", userId, "sid", session.getId(), "reason", reason);
                    return true;
                }
            } catch (Exception e) {
                log.warn("메시지 전송 실패 - crewId: {}, userId: {}", crewId, userId, e);
                sessionManager.removeSession(session);
                closeRemoved(session, "send_failure");
                tally[3]++;
                String ex = e.getClass().getSimpleName();
                obs.count("chat_send_failure_total", "exception", ex);
                obs.count("chat_session_removed_total", "reason", "send_failure");
                obs.trace("SEND_FAIL", "bid", bid, "kind", kind, "user", userId, "sid", session.getId(),
                        "open", session.isOpen(), "ex", ex, "msg", String.valueOf(e.getMessage()).replace('\t', ' '));
                return true; // 오류 발생한 세션은 제거
            }
        });
        obs.count("chat_broadcast_recipients_total", tally[0], "kind", kind);
        obs.count("chat_membership_query_total", tally[1], "site", "broadcast_" + kind);
        obs.count("chat_send_total", tally[2], "result", "ok");
        obs.count("chat_send_total", tally[3], "result", "fail");
        obs.trace("BCAST", "bid", bid, "kind", kind, "crew", crewId, "actor", message.getSenderId(),
                "mapStart", mapAtStart, "mapEnd", crewSessions.size(), "considered", tally[0],
                "mq", tally[1], "ok", tally[2], "fail", tally[3], "removedOther", tally[4],
                "startMs", String.format("%.3f", startMs), "durMs", String.format("%.3f", obs.nowMs() - startMs),
                "qMs", String.format("%.3f", spentNs[0] / 1e6), "sendMs", String.format("%.3f", spentNs[1] / 1e6));
    }

    private void sendErrorMessage(WebSocketSession session, String error) {
        try {
            ChatMessage errorMessage = ChatMessage.builder()
                    .message(error)
                    .messageType(CrewChatEntity.MessageType.SYSTEM)
                    .timestamp(java.time.Instant.now())
                    .build();

            outboundOf(session).sendMessage(new TextMessage(objectMapper.writeValueAsString(errorMessage)));
        } catch (Exception e) {
            log.error("에러 메시지 전송 실패", e);
        }
    }

    private Long extractCrewIdFromUri(String uri) {
        try {
            // URI 형식: /ws/crew/{crewId}/chat
            String[] parts = uri.split("/");
            for (int i = 0; i < parts.length - 1; i++) {
                if ("crew".equals(parts[i]) && i + 1 < parts.length) {
                    return Long.parseLong(parts[i + 1]);
                }
            }
        } catch (Exception e) {
            log.error("크루 ID 추출 실패: {}", uri, e);
        }
        return null;
    }

    private Long extractUserIdFromSession(WebSocketSession session) {
        // WebSocketAuthInterceptor에서 설정한 userId 사용
        Object userId = session.getAttributes().get("userId");
        if (userId instanceof Long) {
            return (Long) userId;
        }

        log.error("WebSocket 세션에서 userId를 찾을 수 없습니다. 인증 실패");
        return null;
    }

    private String extractNicknameFromSession(WebSocketSession session) {
        // WebSocketAuthInterceptor가 핸드셰이크 시점에 조회해 캐싱해둔 닉네임 재사용
        Object nickname = session.getAttributes().get("nickname");
        return nickname instanceof String ? (String) nickname : "Unknown";
    }
}
