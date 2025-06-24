package cmf.commitField.global.kafka.controller;

import cmf.commitField.domain.user.entity.CustomOAuth2User;
import cmf.commitField.domain.user.entity.User;
import cmf.commitField.domain.user.repository.UserRepository;
import cmf.commitField.global.kafka.message.CommitUpdateMessage;
import cmf.commitField.global.kafka.message.NotificationMessage;
import cmf.commitField.global.kafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka/test")
@RequiredArgsConstructor
@Slf4j
public class KafkaTestController {

    private final KafkaProducerService kafkaProducerService;
    private final UserRepository userRepository;

    /**
     * 로그인 없이 테스트 가능한 알림 API
     */
    @PostMapping("/notification/simple")
    public ResponseEntity<?> testNotificationSimple(
            @RequestParam(defaultValue = "testuser") String username,
            @RequestParam(defaultValue = "RANK") String notiType,
            @RequestParam(defaultValue = "RANK_UP") String notiDetailType,
            @RequestParam(defaultValue = "테스트 알림 메시지입니다!") String message) {

        try {
            // 테스트용 가짜 사용자 데이터
            NotificationMessage kafkaMessage = NotificationMessage.builder()
                    .username(username)
                    .userId("1")
                    .notiType(notiType)
                    .notiDetailType(notiDetailType)
                    .message(message)
                    .relId(0L)
                    .relTypeCode("TEST")
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendNotificationMessage(kafkaMessage);

            log.info("테스트 알림 메시지 전송: user={}, message={}", username, message);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "알림 메시지가 Kafka로 전송되었습니다.",
                    "data", kafkaMessage
            ));

        } catch (Exception e) {
            log.error("테스트 알림 메시지 전송 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("메시지 전송 중 오류가 발생했습니다: " + e.getMessage());
        }
    }

    /**
     * 🔥 로그인 없이 테스트 가능한 커밋 업데이트 API
     */
    @PostMapping("/commit-update/simple")
    public ResponseEntity<?> testCommitUpdateSimple(
            @RequestParam(defaultValue = "testuser") String username,
            @RequestParam(defaultValue = "5") long commitCount,
            @RequestParam(defaultValue = "ALL") String updateType) {

        try {
            // 테스트용 가짜 사용자 데이터
            CommitUpdateMessage kafkaMessage = CommitUpdateMessage.builder()
                    .username(username)
                    .userId(1L)
                    .newCommitCount(commitCount)
                    .totalCommitCount(100L + commitCount)
                    .updateType(updateType)
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendCommitUpdateMessage(kafkaMessage);

            log.info("✅ 테스트 커밋 업데이트 메시지 전송: user={}, commits={}", username, commitCount);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "커밋 업데이트 메시지가 Kafka로 전송되었습니다.",
                    "data", kafkaMessage
            ));

        } catch (Exception e) {
            log.error("테스트 커밋 업데이트 메시지 전송 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("메시지 전송 중 오류가 발생했습니다: " + e.getMessage());
        }
    }

    /**
     * Kafka 상태 확인 API
     */
    @GetMapping("/status")
    public ResponseEntity<?> kafkaStatus() {
        try {
            return ResponseEntity.ok(Map.of(
                    "status", "Kafka Test Controller is running",
                    "timestamp", LocalDateTime.now(),
                    "endpoints", Map.of(
                            "notification", "/api/kafka/test/notification/simple",
                            "commit-update", "/api/kafka/test/commit-update/simple"
                    )
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("상태 확인 중 오류: " + e.getMessage());
        }
    }

    /**
     * 알림 메시지 테스트 전송
     */
    @PostMapping("/notification")
    public ResponseEntity<?> testNotification(
            @AuthenticationPrincipal CustomOAuth2User oAuth2User,
            @RequestParam(defaultValue = "RANK") String notiType,
            @RequestParam(defaultValue = "RANK_UP") String notiDetailType,
            @RequestParam(defaultValue = "테스트 알림 메시지입니다!") String message) {

        try {
            String username = oAuth2User.getName();
            User user = userRepository.findByUsername(username).orElse(null);

            if (user == null) {
                return ResponseEntity.badRequest().body("사용자를 찾을 수 없습니다.");
            }

            NotificationMessage kafkaMessage = NotificationMessage.builder()
                    .username(username)
                    .userId(String.valueOf(user.getId()))
                    .notiType(notiType)
                    .notiDetailType(notiDetailType)
                    .message(message)
                    .relId(0L)
                    .relTypeCode("TEST")
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendNotificationMessage(kafkaMessage);

            log.info("테스트 알림 메시지 전송: user={}, message={}", username, message);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "알림 메시지가 Kafka로 전송되었습니다.",
                    "data", kafkaMessage
            ));

        } catch (Exception e) {
            log.error("테스트 알림 메시지 전송 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("메시지 전송 중 오류가 발생했습니다.");
        }
    }

    /**
     * 커밋 업데이트 메시지 테스트 전송
     */
    @PostMapping("/commit-update")
    public ResponseEntity<?> testCommitUpdate(
            @AuthenticationPrincipal CustomOAuth2User oAuth2User,
            @RequestParam(defaultValue = "5") long commitCount,
            @RequestParam(defaultValue = "ALL") String updateType) {

        try {
            String username = oAuth2User.getName();
            User user = userRepository.findByUsername(username).orElse(null);

            if (user == null) {
                return ResponseEntity.badRequest().body("사용자를 찾을 수 없습니다.");
            }

            CommitUpdateMessage kafkaMessage = CommitUpdateMessage.builder()
                    .username(username)
                    .userId(user.getId())
                    .newCommitCount(commitCount)
                    .totalCommitCount(user.getCommitCount() + commitCount)
                    .updateType(updateType)
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendCommitUpdateMessage(kafkaMessage);

            log.info("테스트 커밋 업데이트 메시지 전송: user={}, commits={}", username, commitCount);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "커밋 업데이트 메시지가 Kafka로 전송되었습니다.",
                    "data", kafkaMessage
            ));

        } catch (Exception e) {
            log.error("테스트 커밋 업데이트 메시지 전송 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("메시지 전송 중 오류가 발생했습니다.");
        }
    }

    /**
     * 사용자별 실시간 알림 테스트 전송
     */
    @PostMapping("/user-notification")
    public ResponseEntity<?> testUserNotification(
            @AuthenticationPrincipal CustomOAuth2User oAuth2User,
            @RequestParam(defaultValue = "실시간 테스트 메시지") String data) {

        try {
            String username = oAuth2User.getName();

            kafkaProducerService.sendUserNotification(username, Map.of(
                    "type", "TEST",
                    "message", data,
                    "timestamp", LocalDateTime.now()
            ));

            log.info("테스트 사용자별 실시간 알림 전송: user={}, data={}", username, data);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "사용자별 실시간 알림이 Kafka로 전송되었습니다.",
                    "username", username,
                    "data", data
            ));

        } catch (Exception e) {
            log.error("테스트 사용자별 실시간 알림 전송 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("메시지 전송 중 오류가 발생했습니다.");
        }
    }
}