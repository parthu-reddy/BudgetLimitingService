package com.fooddelivery.advertisement.pacing.scheduler;

import com.fooddelivery.advertisement.pacing.service.PacingEngineService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.Arrays;

@Component
public class BudgetPacingScheduler {

    private final PacingEngineService pacingEngineService;
    private final org.springframework.data.redis.core.StringRedisTemplate redisTemplate;
    
    public BudgetPacingScheduler(PacingEngineService pacingEngineService, org.springframework.data.redis.core.StringRedisTemplate redisTemplate) {
        this.pacingEngineService = pacingEngineService;
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedDelayString = "${pacing.interval.ms:60000}")
    public void runPacingEvaluation() {
        com.fooddelivery.common.lock.RedisLock _redisLock = new com.fooddelivery.common.lock.RedisLock(redisTemplate);
        String _lockToken = java.util.UUID.randomUUID().toString();
        boolean locked = _redisLock.tryAcquire("lock:pacing_job", _lockToken, java.time.Duration.ofSeconds(50));
        if (!locked) { return; }
        
        try {
            org.springframework.data.redis.core.ScanOptions options = org.springframework.data.redis.core.ScanOptions.scanOptions().match("*").count(100).build();
            try (org.springframework.data.redis.core.Cursor<String> cursor = redisTemplate.opsForSet().scan(com.fooddelivery.common.constants.RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, options)) {
                java.util.List<String> batch = new java.util.ArrayList<>();
                while (cursor.hasNext()) {
                    batch.add(cursor.next());
                    if (batch.size() >= 100) {
                        pacingEngineService.evaluatePacingForCampaigns(new java.util.ArrayList<>(batch));
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    pacingEngineService.evaluatePacingForCampaigns(batch);
                }
            } catch (Exception e) {
                // handle error
                e.printStackTrace();
            }
        } finally {
            _redisLock.release("lock:pacing_job", _lockToken);
        }
    }
}
