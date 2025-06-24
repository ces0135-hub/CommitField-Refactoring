package cmf.commitField.global.kafka.message;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommitUpdateMessage {
    private String username;
    private Long userId;
    private long newCommitCount;
    private long totalCommitCount;
    private String updateType; // "USER_EXP", "PET_EXP", "TIER_UPDATE" 등

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;

    // 추가 메타데이터
    private String source = "commit-service";
    private String version = "1.0";
}