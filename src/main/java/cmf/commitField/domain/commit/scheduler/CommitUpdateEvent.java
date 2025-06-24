package cmf.commitField.domain.commit.scheduler;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class CommitUpdateEvent extends ApplicationEvent {
    private final String username;
    private final long newCommitCount;

    public CommitUpdateEvent(Object source, String username, long newCommitCount) {
        super(source);
        this.username = username;
        this.newCommitCount = newCommitCount;
    }

    // 🔄 기존 이벤트는 유지하되, 점진적으로 Kafka로 전환 중임을 표시
    @Override
    public String toString() {
        return "CommitUpdateEvent{" +
                "username='" + username + '\'' +
                ", newCommitCount=" + newCommitCount +
                ", status='MIGRATING_TO_KAFKA'" +
                '}';
    }
}