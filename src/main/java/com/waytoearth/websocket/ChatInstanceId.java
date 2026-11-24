package com.waytoearth.websocket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 이 JVM 인스턴스를 식별하는 값. 크루 채팅 Redis Pub/Sub 릴레이에서
 * 자기 자신이 발행한 메시지를 재구독해 중복 처리하지 않도록 구분하는 용도로만 쓰인다.
 * 프로덕션에서는 배포 시 EC2 인스턴스 ID 등으로 INSTANCE_ID를 주입하는 걸 권장하고,
 * 로컬/단일 인스턴스 환경에서는 기동 시 한 번 생성되는 랜덤 UUID로 대체된다.
 */
@Component
public class ChatInstanceId {

    private final String value;

    public ChatInstanceId(@Value("${INSTANCE_ID:${random.uuid}}") String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
