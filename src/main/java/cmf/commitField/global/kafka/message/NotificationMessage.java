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
public class NotificationMessage {
    private String username;
    private String userId;
    private String notiType;        // NotiType enum의 문자열 값
    private String notiDetailType;  // NotiDetailType enum의 문자열 값
    private String message;
    private Long relId;
    private String relTypeCode;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;

    // 추가 메타데이터
    private String source = "notification-service";
    private String version = "1.0";
}