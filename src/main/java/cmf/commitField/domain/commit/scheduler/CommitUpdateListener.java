package cmf.commitField.domain.commit.scheduler;

import cmf.commitField.domain.pet.service.PetService;
import cmf.commitField.domain.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CommitUpdateListener {
    private final UserService userService;
    private final PetService petService;
    private final CommitUpdateService commitUpdateService;
    private final SimpMessagingTemplate messagingTemplate;

    // 🔄 Kafka로 전환했으므로 주석처리 (점진적 전환을 위해 삭제하지 않음)
    // @EventListener
    public void handleCommitUserUpdateEvent(CommitUpdateEvent event) {
        String username = event.getUsername();
        long commitCount = event.getNewCommitCount();

        System.out.println("[DEPRECATED] 유저 시즌 경험치 업데이트: " + username + " - Kafka로 전환됨");

        // 기존 로직 주석처리
        // boolean levelUp = userService.getExpUser(username,commitCount);
        // if(levelUp) commitUpdateService.updateUserTier(username);
        // userService.updateUserCommitCount(username, commitCount);
        // System.out.println("유저명: " + username + " has updated " + commitCount + " commits.");
    }

    // @EventListener
    public void handleCommitPetUpdateEvent(CommitUpdateEvent event) {
        String username = event.getUsername();
        long commitCount = event.getNewCommitCount();

        System.out.println("[DEPRECATED] 유저 펫 경험치 업데이트: " + username + " - Kafka로 전환됨");

        // 기존 로직 주석처리
        // petService.getExpPet(username,commitCount);
        // System.out.println("유저명: " + username + "'s pet has updated " + commitCount + " commits.");
    }

    // @EventListener
    public void onCommitCountUpdate(CommitUpdateEvent event) {
        String username = event.getUsername();
        long newCommitCount = event.getNewCommitCount();

        System.out.println("[DEPRECATED] 커밋 수 업데이트 WebSocket: " + username + " - Kafka로 전환됨");

        // 기존 로직 주석처리
        // messagingTemplate.convertAndSend("/topic/commit/" + username, newCommitCount);
    }
}