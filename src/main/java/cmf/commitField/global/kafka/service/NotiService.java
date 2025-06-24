package cmf.commitField.domain.noti.noti.service;

import cmf.commitField.domain.noti.noti.dto.NotiDto;
import cmf.commitField.domain.noti.noti.entity.Noti;
import cmf.commitField.domain.noti.noti.entity.NotiDetailType;
import cmf.commitField.domain.noti.noti.entity.NotiType;
import cmf.commitField.domain.noti.noti.event.NotiEvent;
import cmf.commitField.domain.noti.noti.repository.NotiRepository;
import cmf.commitField.domain.season.entity.Season;
import cmf.commitField.domain.user.entity.User;
import cmf.commitField.global.error.ErrorCode;
import cmf.commitField.global.exception.CustomException;
import cmf.commitField.global.kafka.message.NotificationMessage;
import cmf.commitField.global.kafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
@Slf4j
public class NotiService {
    private final NotiRepository notiRepository;
    private final ApplicationEventPublisher eventPublisher; // 기존 방식 유지 (점진적 전환)
    private final KafkaProducerService kafkaProducerService; // Kafka 추가

    /**
     * 알림 생성 공통 로직 - Kafka 적용 버전
     */
    @Transactional
    public void createNoti(User receiver, NotiType notiType, NotiDetailType notiDetailType,
                           Long relId, String relTypeCode, Object... params) {
        // 메시지 생성
        String message = notiDetailType.formatMessage(params);

        // 알림 엔티티 생성 및 저장
        Noti noti = Noti.builder()
                .typeCode(notiType)
                .type2Code(notiDetailType)
                .receiver(receiver)
                .isRead(false)
                .message(message)
                .relId(relId)
                .relTypeCode(relTypeCode)
                .build();

        notiRepository.save(noti);

        // Kafka 메시지 발송 (리팩토링)
        NotificationMessage kafkaMessage = NotificationMessage.builder()
                .username(receiver.getUsername())
                .userId(String.valueOf(receiver.getId()))
                .notiType(notiType.name())
                .notiDetailType(notiDetailType.name())
                .message(message)
                .relId(relId)
                .relTypeCode(relTypeCode)
                .timestamp(LocalDateTime.now())
                .build();

        kafkaProducerService.sendNotificationMessage(kafkaMessage);

        // 🔄 기존 WebSocket 이벤트도 병행 유지 (점진적 전환)
        List<NotiDto> notis = new ArrayList<>();
        notis.add(new NotiDto(noti.getId(), noti.getMessage(), noti.getCreatedAt()));
        NotiEvent event = new NotiEvent(this, receiver.getUsername(), notis, "새로운 알림이 생성되었습니다.");
        eventPublisher.publishEvent(event);

        log.info("알림 생성 및 Kafka 메시지 발송 완료: user={}, type={}",
                receiver.getUsername(), notiDetailType.name());
    }

    /**
     * 알림 조회
     */
    public List<NotiDto> getNotReadNoti(User receiver) {
        List<NotiDto> notis = notiRepository.findNotiDtoByReceiverId(receiver.getId(), false).orElse(null);
        return notis;
    }

    /**
     * 시즌 알림 확인
     */
    public List<Noti> getSeasonNotiCheck(User receiver, long seasonId) {
        return notiRepository.findNotiByReceiverAndRelId(receiver, seasonId)
                .orElseThrow(() -> new CustomException(ErrorCode.ERROR_CHECK));
    }

    /**
     * 새 시즌 알림 생성
     */
    @Transactional
    public void createNewSeasonNoti(Season season, User user) {
        createNoti(user, NotiType.SEASON, NotiDetailType.SEASON_START,
                season.getId(), season.getModelName(), season.getName());
    }

    /**
     * 랭킹 업 알림 생성
     */
    @Transactional
    public void createRankUpNoti(User user) {
        createNoti(user, NotiType.RANK, NotiDetailType.RANK_UP,
                0L, null, getDisplayName(user), user.getTier().name());
    }

    /**
     * 연속 커밋 축하 알림 생성
     */
    @Transactional
    public void createStreakCommitNoti(User user, String days) {
        createNoti(user, NotiType.STREAK, NotiDetailType.STREAK_CONTINUED,
                0L, null, getDisplayName(user), days);
    }

    /**
     * 커밋 부재 알림 생성
     */
    @Transactional
    public void createStreakBrokenNoti(User user) {
        createNoti(user, NotiType.STREAK, NotiDetailType.STREAK_BROKEN,
                0L, null, getDisplayName(user));
    }

    /**
     * 업적 알림 생성
     */
    @Transactional
    public void createAchievementNoti(User user, String achievementName) {
        createNoti(user, NotiType.ACHIEVEMENT, NotiDetailType.ACHIEVEMENT_COMPLETED,
                0L, null, getDisplayName(user), achievementName);
    }

    /**
     * 공지사항 알림 생성
     */
    @Transactional
    public void createNoticeNoti(User user, String noticeTitle) {
        createNoti(user, NotiType.NOTICE, NotiDetailType.NOTICE_CREATED,
                0L, null, noticeTitle);
    }

    /**
     * 알림 읽음 처리
     */
    @Transactional
    public void read(User receiver) {
        log.info("read notis");
        List<Noti> notis = notiRepository.findNotiByReceiver(receiver)
                .orElseThrow(() -> new CustomException(ErrorCode.NOT_FOUND_USER));
        notis.forEach(noti -> {
            noti.setRead(true);
        });
    }

    /**
     * 닉네임 여부에 따라 표시 이름 반환
     */
    private String getDisplayName(User user) {
        return user.getNickname() != null ? user.getNickname() : user.getUsername();
    }
}