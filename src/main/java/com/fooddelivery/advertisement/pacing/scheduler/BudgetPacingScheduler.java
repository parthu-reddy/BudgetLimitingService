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

    // Run every minute
    @Scheduled(fixedRateString = "${pacing.interval.ms:60000}")
    public void runPacingEvaluation() {
        java.util.Set<String> activeCampaigns = redisTemplate.opsForSet().members(com.fooddelivery.common.constants.RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS);
        if (activeCampaigns != null && !activeCampaigns.isEmpty()) {
            pacingEngineService.evaluatePacingForCampaigns(new java.util.ArrayList<>(activeCampaigns));
        }
    }
}
