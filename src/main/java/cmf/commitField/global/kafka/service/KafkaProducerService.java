package cmf.commitField.global.kafka.service;

import cmf.commitField.global.kafka.message.CommitUpdateMessage;
import cmf.commitField.global.kafka.message.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    // 토픽 상수들
    public static final String NOTIFICATION_TOPIC = "notification-events";
    public static final String COMMIT_UPDATE_TOPIC = "commit-update-events";
    public static final String USER_SPECIFIC_NOTIFICATION_TOPIC = "user-notifications";

    /**
     * 알림 메시지 발송
     */
    public void sendNotificationMessage(NotificationMessage message) {
        try {
            // 파티션 키로 사용자명 사용 (같은 사용자의 알림은 순서 보장)
            String partitionKey = message.getUsername();

            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(NOTIFICATION_TOPIC, partitionKey, message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("알림 메시지 전송 성공: user={}, topic={}, offset={}",
                            message.getUsername(),
                            NOTIFICATION_TOPIC,
                            result.getRecordMetadata().offset());
                } else {
                    log.error("알림 메시지 전송 실패: user={}, error={}",
                            message.getUsername(), ex.getMessage());
                }
            });

        } catch (Exception e) {
            log.error("알림 메시지 발송 중 예외 발생: {}", e.getMessage(), e);
        }
    }

    /**
     * 커밋 업데이트 메시지 발송
     */
    public void sendCommitUpdateMessage(CommitUpdateMessage message) {
        try {
            // 파티션 키로 사용자명 사용
            String partitionKey = message.getUsername();

            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(COMMIT_UPDATE_TOPIC, partitionKey, message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("커밋 업데이트 메시지 전송 성공: user={}, commitCount={}, offset={}",
                            message.getUsername(),
                            message.getNewCommitCount(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("커밋 업데이트 메시지 전송 실패: user={}, error={}",
                            message.getUsername(), ex.getMessage());
                }
            });

        } catch (Exception e) {
            log.error("커밋 업데이트 메시지 발송 중 예외 발생: {}", e.getMessage(), e);
        }
    }

    /**
     * 사용자별 실시간 알림 발송 (WebSocket용)
     */
    public void sendUserNotification(String username, Object notificationData) {
        try {
            String topicName = USER_SPECIFIC_NOTIFICATION_TOPIC + "-" + username;

            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topicName, username, notificationData);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("사용자 실시간 알림 전송 성공: user={}", username);
                } else {
                    log.error("사용자 실시간 알림 전송 실패: user={}, error={}",
                            username, ex.getMessage());
                }
            });

        } catch (Exception e) {
            log.error("사용자 실시간 알림 발송 중 예외 발생: {}", e.getMessage(), e);
        }
    }
}