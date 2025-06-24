package cmf.commitField.domain.commit.scheduler;

import cmf.commitField.domain.commit.totalCommit.service.TotalCommitService;
import cmf.commitField.domain.noti.noti.service.CommitSteakNotiService;
import cmf.commitField.domain.user.entity.User;
import cmf.commitField.domain.user.repository.UserRepository;
import cmf.commitField.global.kafka.message.CommitUpdateMessage;
import cmf.commitField.global.kafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommitScheduler {
    private final TotalCommitService totalCommitService;
    private final UserRepository userRepository;
    private final StringRedisTemplate redisTemplate;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final CommitSteakNotiService commitSteakNotiService;
    private final ApplicationEventPublisher eventPublisher; // 기존 방식 병행 유지
    private final KafkaProducerService kafkaProducerService; // 🔥 Kafka 추가

    @Scheduled(fixedRate = 60000) // 1분마다 실행
    public void updateUserCommits() {
        log.info("🔍 updateUserCommits 실행중 - Kafka 적용 버전");
        int count = counter.incrementAndGet();

        Set<String> activeUsers = redisTemplate.keys("commit_active:*");
        log.info("🔍 Active User Count: {}", activeUsers.size());

        for (String key : activeUsers) {
            String username = key.replace("commit_active:", "");

            String lastcmKey = "commit_lastCommitted:" + username;
            String lastCommitted = redisTemplate.opsForValue().get(lastcmKey);

            System.out.println("username: " + username + "/ user lastCommitted: " + lastCommitted);
            if (username != null && lastCommitted != null) {
                processUserCommit(username);
            }
        }
    }

    // 🔥 유저 커밋 검사 및 반영 - Kafka 적용
    private void processUserCommit(String username) {
        String activeKey = "commit_active:" + username;
        String lastcmKey = "commit_lastCommitted:" + username;
        Long currentCommit = Long.parseLong(redisTemplate.opsForValue().get(activeKey));
        String lastcommitted = redisTemplate.opsForValue().get(lastcmKey);
        long updateTotalCommit, newCommitCount;

        updateTotalCommit = totalCommitService.getTotalCommitCount(username).getTotalCommitContributions();
        int currentStreakCommit = totalCommitService.getTotalCommitCount(username).getCurrentStreakDays();

        newCommitCount = updateTotalCommit - currentCommit;

        if (newCommitCount > 0) {
            User user = userRepository.findByUsername(username).get();
            LocalDateTime now = LocalDateTime.now();

            user.setLastCommitted(now);
            userRepository.save(user);

            redisTemplate.opsForValue().set(activeKey, String.valueOf(updateTotalCommit), 3, TimeUnit.HOURS);
            redisTemplate.opsForValue().set(lastcmKey, String.valueOf(now), 3, TimeUnit.HOURS);

            // 🔥 Kafka로 커밋 업데이트 이벤트 발송 (새로운 방식)
            sendCommitUpdateToKafka(username, user.getId(), newCommitCount, updateTotalCommit);

            // 🔄 기존 ApplicationEvent도 병행 유지 (점진적 전환)
            CommitUpdateEvent event = new CommitUpdateEvent(this, username, newCommitCount);
            eventPublisher.publishEvent(event);

            commitSteakNotiService.checkAndCreateSteakNoti(user, currentStreakCommit);

            log.info("커밋 업데이트 완료 - Kafka 발송: user={}, newCommits={}, total={}",
                    username, newCommitCount, updateTotalCommit);

        } else if (newCommitCount < 0) {
            redisTemplate.opsForValue().set(activeKey, String.valueOf(updateTotalCommit), 3, TimeUnit.HOURS);

            // 동기화 필요한 경우에도 Kafka 발송
            sendCommitUpdateToKafka(username, null, newCommitCount, updateTotalCommit);

            CommitUpdateEvent event = new CommitUpdateEvent(this, username, newCommitCount);
            eventPublisher.publishEvent(event);

            log.info("커밋 수 동기화 - Kafka 발송: user={}", username);
        }

        log.info("User: {}, LastCommitted: {}, New Commits: {}, Total Commits: {}",
                username, lastcommitted, newCommitCount, updateTotalCommit);
    }

    /**
     * Kafka로 커밋 업데이트 메시지 발송
     */
    private void sendCommitUpdateToKafka(String username, Long userId, long newCommitCount, long totalCommitCount) {
        try {
            // 사용자별로 다른 업데이트 타입으로 메시지 발송

            // 1. 유저 경험치 업데이트
            CommitUpdateMessage userExpMessage = CommitUpdateMessage.builder()
                    .username(username)
                    .userId(userId)
                    .newCommitCount(newCommitCount)
                    .totalCommitCount(totalCommitCount)
                    .updateType("USER_EXP")
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendCommitUpdateMessage(userExpMessage);

            // 2. 펫 경험치 업데이트
            CommitUpdateMessage petExpMessage = CommitUpdateMessage.builder()
                    .username(username)
                    .userId(userId)
                    .newCommitCount(newCommitCount)
                    .totalCommitCount(totalCommitCount)
                    .updateType("PET_EXP")
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendCommitUpdateMessage(petExpMessage);

            // 3. 커밋 수 업데이트
            CommitUpdateMessage commitCountMessage = CommitUpdateMessage.builder()
                    .username(username)
                    .userId(userId)
                    .newCommitCount(newCommitCount)
                    .totalCommitCount(totalCommitCount)
                    .updateType("COMMIT_COUNT_UPDATE")
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendCommitUpdateMessage(commitCountMessage);

            log.info("Kafka 커밋 업데이트 메시지 발송 완료: user={}, commits={}", username, newCommitCount);

        } catch (Exception e) {
            log.error("Kafka 커밋 업데이트 메시지 발송 실패: user={}, error={}", username, e.getMessage(), e);
        }
    }
}