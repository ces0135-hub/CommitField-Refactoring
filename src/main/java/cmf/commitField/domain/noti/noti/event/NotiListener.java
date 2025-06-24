package cmf.commitField.domain.noti.noti.event;

import lombok.RequiredArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class NotiListener {
    private final SimpMessagingTemplate messagingTemplate;

    // 🔄 Kafka로 전환했으므로 주석처리 (점진적 전환을 위해 삭제하지 않음)
    // @EventListener
    public void handleNotiEvent(NotiEvent event) {
        String username = event.getUsername();
        System.out.println("🔄 [DEPRECATED] NotiEvent: " + event.getMessage() + " - Kafka로 전환됨");
        // messagingTemplate.convertAndSend("/topic/notifications/" + username, event.getNotis());
    }
}