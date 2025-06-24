package cmf.commitField.global.kafka.consumer;

import cmf.commitField.domain.noti.noti.dto.NotiDto;
import cmf.commitField.global.kafka.message.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationKafkaConsumer {

    private final SimpMessagingTemplate messagingTemplate;

    /**
     * 알림 이벤트 처리 - 기존 NotiListener.handleNotiEvent() 대체
     */
    @KafkaListener(
            topics = "notification-events",
            groupId = "notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleNotificationEvent(
            @Payload NotificationMessage message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            log.info("🔔 알림 이벤트 수신: user={}, type={}, topic={}, partition={}, offset={}",
                    message.getUsername(),
                    message.getNotiDetailType(),
                    topic,
                    partition,
                    offset);

            // 기존 NotiListener의 로직을 여기서 처리
            String username = message.getUsername();

            // WebSocket으로 실시간 알림 전송
            List<NotiDto> notis = new ArrayList<>();
            notis.add(new NotiDto(
                    Long.parseLong(message.getUserId()),
                    message.getMessage(),
                    message.getTimestamp()
            ));

            // WebSocket 토픽으로 알림 전송
            messagingTemplate.convertAndSend("/topic/notifications/" + username, notis);

            log.info("WebSocket 알림 전송 완료: user={}, message={}", username, message.getMessage());

            // 수동 커밋
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("알림 이벤트 처리 중 오류 발생: user={}, error={}",
                    message.getUsername(), e.getMessage(), e);
            // 에러 발생 시에도 acknowledge (무한 재시도 방지)
            acknowledgment.acknowledge();
        }
    }

    /**
     * 사용자별 실시간 알림 처리
     */
    @KafkaListener(
            topicPattern = "user-notifications-.*",
            groupId = "user-notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleUserSpecificNotification(
            @Payload Object notificationData,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            // 토픽명에서 사용자명 추출 (user-notifications-{username})
            String username = topic.substring("user-notifications-".length());

            log.info("사용자별 실시간 알림 수신: user={}, topic={}, partition={}, offset={}",
                    username, topic, partition, offset);

            // WebSocket으로 실시간 데이터 전송
            messagingTemplate.convertAndSend("/topic/user/" + username, notificationData);

            log.info("사용자별 실시간 알림 전송 완료: user={}", username);

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("사용자별 실시간 알림 처리 중 오류 발생: topic={}, error={}",
                    topic, e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
}