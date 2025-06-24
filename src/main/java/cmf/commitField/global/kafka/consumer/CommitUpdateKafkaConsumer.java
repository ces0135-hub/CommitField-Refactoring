package cmf.commitField.global.kafka.consumer;

import cmf.commitField.domain.pet.service.PetService;
import cmf.commitField.domain.user.service.UserService;
import cmf.commitField.global.kafka.message.CommitUpdateMessage;
import cmf.commitField.domain.commit.scheduler.CommitUpdateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CommitUpdateKafkaConsumer {

    private final UserService userService;
    private final PetService petService;
    private final CommitUpdateService commitUpdateService;
    private final SimpMessagingTemplate messagingTemplate;

    /**
     * 커밋 업데이트 이벤트 처리 - 기존 CommitUpdateListener 대체
     */
    @KafkaListener(
            topics = "commit-update-events",
            groupId = "commit-update-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleCommitUpdateEvent(
            @Payload CommitUpdateMessage message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            String username = message.getUsername();
            long commitCount = message.getNewCommitCount();
            String updateType = message.getUpdateType();

            log.info("📈 커밋 업데이트 이벤트 수신: user={}, commitCount={}, type={}, partition={}, offset={}",
                    username, commitCount, updateType, partition, offset);

            // 업데이트 타입에 따라 처리 분기
            switch (updateType) {
                case "USER_EXP":
                    handleUserExpUpdate(username, commitCount);
                    break;
                case "PET_EXP":
                    handlePetExpUpdate(username, commitCount);
                    break;
                case "COMMIT_COUNT_UPDATE":
                    handleCommitCountUpdate(username, commitCount);
                    break;
                case "ALL": // 기존 방식과 호환
                default:
                    handleAllUpdates(username, commitCount);
                    break;
            }

            // WebSocket으로 실시간 커밋 업데이트 알림
            messagingTemplate.convertAndSend("/topic/commit/" + username, commitCount);

            log.info("커밋 업데이트 처리 완료: user={}, commitCount={}", username, commitCount);

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("커밋 업데이트 처리 중 오류 발생: user={}, error={}",
                    message.getUsername(), e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }

    // 나머지 메서드들은 동일...
    private void handleUserExpUpdate(String username, long commitCount) {
        log.info("유저 시즌 경험치 업데이트: {}", username);
        boolean levelUp = userService.getExpUser(username, commitCount);
        if (levelUp) {
            commitUpdateService.updateUserTier(username);
        }
        userService.updateUserCommitCount(username, commitCount);
        log.info("유저 경험치 업데이트 완료: {} (+{})", username, commitCount);
    }

    private void handlePetExpUpdate(String username, long commitCount) {
        log.info("유저 펫 경험치 업데이트: {}", username);
        petService.getExpPet(username, commitCount);
        log.info("펫 경험치 업데이트 완료: {} (+{})", username, commitCount);
    }

    private void handleCommitCountUpdate(String username, long commitCount) {
        log.info("커밋 수 업데이트: {}", username);
        userService.updateUserCommitCount(username, commitCount);
        log.info("커밋 수 업데이트 완료: {} (+{})", username, commitCount);
    }

    private void handleAllUpdates(String username, long commitCount) {
        log.info("전체 업데이트 처리: {}", username);
        boolean levelUp = userService.getExpUser(username, commitCount);
        if (levelUp) {
            commitUpdateService.updateUserTier(username);
        }
        petService.getExpPet(username, commitCount);
        userService.updateUserCommitCount(username, commitCount);
        log.info("전체 업데이트 완료: {} (+{})", username, commitCount);
    }
}