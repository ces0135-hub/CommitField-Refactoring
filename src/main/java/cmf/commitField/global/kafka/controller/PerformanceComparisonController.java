package cmf.commitField.global.kafka.controller;

import cmf.commitField.domain.noti.noti.dto.NotiDto;
import cmf.commitField.domain.noti.noti.entity.NotiDetailType;
import cmf.commitField.domain.noti.noti.entity.NotiType;
import cmf.commitField.domain.noti.noti.event.NotiEvent;
import cmf.commitField.domain.noti.noti.service.GeneralNotiService;
import cmf.commitField.domain.user.repository.UserRepository;
import cmf.commitField.global.kafka.message.NotificationMessage;
import cmf.commitField.global.kafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/api/performance/test")
@RequiredArgsConstructor
@Slf4j
public class PerformanceComparisonController {

    private final KafkaProducerService kafkaProducerService;
    private final ApplicationEventPublisher eventPublisher;
    private final GeneralNotiService generalNotiService;
    private final UserRepository userRepository;

    /**
     * 🔥 기존 방식 vs Kafka 방식 성능 비교 테스트
     */
    @PostMapping("/notification/compare")
    public ResponseEntity<?> compareNotificationPerformance(
            @RequestParam(defaultValue = "100") int messageCount,
            @RequestParam(defaultValue = "5") int threadCount,
            @RequestParam(defaultValue = "testuser") String username) {

        log.info("🚀 성능 비교 테스트 시작: messageCount={}, threadCount={}, user={}",
                messageCount, threadCount, username);

        Map<String, Object> results = new HashMap<>();

        try {
            // 1. 기존 방식 (ApplicationEvent) 테스트
            Map<String, Object> legacyResults = testLegacyApproach(messageCount, threadCount, username);
            results.put("legacy_approach", legacyResults);

            // 잠시 대기 (메모리 정리)
            Thread.sleep(2000);

            // 2. Kafka 방식 테스트
            Map<String, Object> kafkaResults = testKafkaApproach(messageCount, threadCount, username);
            results.put("kafka_approach", kafkaResults);

            // 3. 비교 결과 계산
            Map<String, Object> comparison = calculateComparison(legacyResults, kafkaResults);
            results.put("comparison", comparison);

            log.info("✅ 성능 비교 테스트 완료");
            return ResponseEntity.ok(results);

        } catch (Exception e) {
            log.error("❌ 성능 비교 테스트 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("테스트 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 기존 방식 (ApplicationEvent) 성능 테스트
     */
    private Map<String, Object> testLegacyApproach(int messageCount, int threadCount, String username)
            throws InterruptedException {

        log.info("📊 기존 방식 (ApplicationEvent) 테스트 시작");

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong totalTime = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            final int messageIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                long messageStartTime = System.nanoTime();
                try {
                    // 기존 방식: ApplicationEvent 발행
                    List<NotiDto> notis = new ArrayList<>();
                    notis.add(new NotiDto(1L, "Legacy 테스트 메시지 " + messageIndex, LocalDateTime.now()));

                    NotiEvent event = new NotiEvent(this, username, notis, "Legacy 테스트 알림");
                    eventPublisher.publishEvent(event);

                    successCount.incrementAndGet();

                } catch (Exception e) {
                    log.error("Legacy 메시지 전송 실패 ({}): {}", messageIndex, e.getMessage());
                    errorCount.incrementAndGet();
                } finally {
                    long messageEndTime = System.nanoTime();
                    totalTime.addAndGet(messageEndTime - messageStartTime);
                }
            }, executor);

            futures.add(future);
        }

        // 모든 작업 완료 대기
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long endTime = System.currentTimeMillis();
        long totalDuration = endTime - startTime;

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        Map<String, Object> results = new HashMap<>();
        results.put("method", "ApplicationEvent (Legacy)");
        results.put("total_messages", messageCount);
        results.put("thread_count", threadCount);
        results.put("success_count", successCount.get());
        results.put("error_count", errorCount.get());
        results.put("total_duration_ms", totalDuration);
        results.put("average_message_time_ns", successCount.get() > 0 ? totalTime.get() / successCount.get() : 0);
        results.put("throughput_per_second", successCount.get() > 0 ? (successCount.get() * 1000.0) / totalDuration : 0);
        results.put("success_rate_percent", (successCount.get() * 100.0) / messageCount);

        log.info("📊 Legacy 결과: 성공={}, 실패={}, 소요시간={}ms, 처리율={}/sec",
                successCount.get(), errorCount.get(), totalDuration,
                String.format("%.2f", (successCount.get() * 1000.0) / totalDuration));

        return results;
    }

    /**
     * Kafka 방식 성능 테스트
     */
    private Map<String, Object> testKafkaApproach(int messageCount, int threadCount, String username)
            throws InterruptedException {

        log.info("📊 Kafka 방식 테스트 시작");

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong totalTime = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            final int messageIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                long messageStartTime = System.nanoTime();
                try {
                    // Kafka 방식: 메시지 발송
                    NotificationMessage kafkaMessage = NotificationMessage.builder()
                            .username(username)
                            .userId("1")
                            .notiType(NotiType.RANK.name())
                            .notiDetailType(NotiDetailType.RANK_UP.name())
                            .message("Kafka 테스트 메시지 " + messageIndex)
                            .relId(0L)
                            .relTypeCode("PERFORMANCE_TEST")
                            .timestamp(LocalDateTime.now())
                            .build();

                    kafkaProducerService.sendNotificationMessage(kafkaMessage);

                    successCount.incrementAndGet();

                } catch (Exception e) {
                    log.error("Kafka 메시지 전송 실패 ({}): {}", messageIndex, e.getMessage());
                    errorCount.incrementAndGet();
                } finally {
                    long messageEndTime = System.nanoTime();
                    totalTime.addAndGet(messageEndTime - messageStartTime);
                }
            }, executor);

            futures.add(future);
        }

        // 모든 작업 완료 대기
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long endTime = System.currentTimeMillis();
        long totalDuration = endTime - startTime;

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        Map<String, Object> results = new HashMap<>();
        results.put("method", "Kafka");
        results.put("total_messages", messageCount);
        results.put("thread_count", threadCount);
        results.put("success_count", successCount.get());
        results.put("error_count", errorCount.get());
        results.put("total_duration_ms", totalDuration);
        results.put("average_message_time_ns", successCount.get() > 0 ? totalTime.get() / successCount.get() : 0);
        results.put("throughput_per_second", successCount.get() > 0 ? (successCount.get() * 1000.0) / totalDuration : 0);
        results.put("success_rate_percent", (successCount.get() * 100.0) / messageCount);

        log.info("📊 Kafka 결과: 성공={}, 실패={}, 소요시간={}ms, 처리율={}/sec",
                successCount.get(), errorCount.get(), totalDuration,
                String.format("%.2f", (successCount.get() * 1000.0) / totalDuration));

        return results;
    }

    /**
     * 비교 결과 계산
     */
    private Map<String, Object> calculateComparison(Map<String, Object> legacy, Map<String, Object> kafka) {
        Map<String, Object> comparison = new HashMap<>();

        double legacyThroughput = (Double) legacy.get("throughput_per_second");
        double kafkaThroughput = (Double) kafka.get("throughput_per_second");

        long legacyDuration = (Long) legacy.get("total_duration_ms");
        long kafkaDuration = (Long) kafka.get("total_duration_ms");

        long legacyAvgTime = (Long) legacy.get("average_message_time_ns");
        long kafkaAvgTime = (Long) kafka.get("average_message_time_ns");

        double legacySuccessRate = (Double) legacy.get("success_rate_percent");
        double kafkaSuccessRate = (Double) kafka.get("success_rate_percent");

        // 개선율 계산
        double throughputImprovement = kafkaThroughput > 0 ? ((kafkaThroughput - legacyThroughput) / legacyThroughput) * 100 : 0;
        double durationImprovement = legacyDuration > 0 ? ((double)(legacyDuration - kafkaDuration) / legacyDuration) * 100 : 0;
        double avgTimeImprovement = legacyAvgTime > 0 ? ((double)(legacyAvgTime - kafkaAvgTime) / legacyAvgTime) * 100 : 0;
        double successRateImprovement = legacySuccessRate > 0 ? ((kafkaSuccessRate - legacySuccessRate) / legacySuccessRate) * 100 : 0;

        comparison.put("throughput_improvement_percent", String.format("%.2f", throughputImprovement));
        comparison.put("duration_improvement_percent", String.format("%.2f", durationImprovement));
        comparison.put("avg_time_improvement_percent", String.format("%.2f", avgTimeImprovement));
        comparison.put("success_rate_improvement_percent", String.format("%.2f", successRateImprovement));

        // 우승자 결정
        Map<String, String> winner = new HashMap<>();
        winner.put("throughput", kafkaThroughput > legacyThroughput ? "Kafka" : "Legacy");
        winner.put("duration", kafkaDuration < legacyDuration ? "Kafka" : "Legacy");
        winner.put("avg_time", kafkaAvgTime < legacyAvgTime ? "Kafka" : "Legacy");
        winner.put("success_rate", kafkaSuccessRate > legacySuccessRate ? "Kafka" : "Legacy");

        comparison.put("winner", winner);

        // 종합 점수 (4개 지표 중 몇 개를 Kafka가 이겼는지)
        long kafkaWins = winner.values().stream().mapToLong(w -> "Kafka".equals(w) ? 1 : 0).sum();
        comparison.put("kafka_wins_out_of_4", kafkaWins);
        comparison.put("overall_winner", kafkaWins >= 2 ? "Kafka" : "Legacy");

        return comparison;
    }

    /**
     * 📊 간단한 처리량 테스트
     */
    @PostMapping("/throughput")
    public ResponseEntity<?> throughputTest(
            @RequestParam(defaultValue = "1000") int messageCount,
            @RequestParam(defaultValue = "testuser") String username) {

        try {
            Map<String, Object> results = new HashMap<>();

            // Legacy 처리량 테스트
            long legacyStart = System.currentTimeMillis();
            for (int i = 0; i < messageCount; i++) {
                List<NotiDto> notis = new ArrayList<>();
                notis.add(new NotiDto(1L, "처리량 테스트 " + i, LocalDateTime.now()));
                NotiEvent event = new NotiEvent(this, username, notis, "처리량 테스트");
                eventPublisher.publishEvent(event);
            }
            long legacyEnd = System.currentTimeMillis();
            long legacyDuration = legacyEnd - legacyStart;

            // Kafka 처리량 테스트
            long kafkaStart = System.currentTimeMillis();
            for (int i = 0; i < messageCount; i++) {
                NotificationMessage message = NotificationMessage.builder()
                        .username(username)
                        .userId("1")
                        .notiType("RANK")
                        .notiDetailType("RANK_UP")
                        .message("Kafka 처리량 테스트 " + i)
                        .timestamp(LocalDateTime.now())
                        .build();
                kafkaProducerService.sendNotificationMessage(message);
            }
            long kafkaEnd = System.currentTimeMillis();
            long kafkaDuration = kafkaEnd - kafkaStart;

            results.put("message_count", messageCount);
            results.put("legacy_duration_ms", legacyDuration);
            results.put("kafka_duration_ms", kafkaDuration);
            results.put("legacy_throughput_per_sec", (messageCount * 1000.0) / legacyDuration);
            results.put("kafka_throughput_per_sec", (messageCount * 1000.0) / kafkaDuration);
            results.put("improvement_percent", ((double)(legacyDuration - kafkaDuration) / legacyDuration) * 100);

            return ResponseEntity.ok(results);

        } catch (Exception e) {
            log.error("처리량 테스트 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("테스트 실패: " + e.getMessage());
        }
    }
}