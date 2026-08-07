package com.fooddelivery.advertisement.pacing.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.stereotype.Service;
import com.fooddelivery.common.constants.RedisKeyConstants;
import com.fooddelivery.advertisement.pacing.client.CampaignClient;
import com.fooddelivery.advertisement.pacing.dto.CampaignPacingDTO;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import com.fooddelivery.common.service.NotificationRouterService;
import com.fooddelivery.common.event.NotificationRequestEvent;
import com.fooddelivery.common.enums.ChannelType;

@Service
public class PacingEngineService {
    @java.lang.SuppressWarnings("all")
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PacingEngineService.class);
    private final StringRedisTemplate redisTemplate;
    private final CampaignClient campaignClient;
    private final NotificationRouterService notificationRouterService;
    // Lower bound floor to prevent 0% win rate
    private static final double S_MIN = 0.1;

    public PacingEngineService(StringRedisTemplate redisTemplate, CampaignClient campaignClient, NotificationRouterService notificationRouterService) {
        this.redisTemplate = redisTemplate;
        this.campaignClient = campaignClient;
        this.notificationRouterService = notificationRouterService;
    }

    public void evaluatePacingForCampaigns(List<String> activeCampaignIds) {
        if (activeCampaignIds == null || activeCampaignIds.isEmpty()) return;
        // 1. Pipeline Get all current multipliers and daily spends
        List<Object> pipelineResults = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String campaignId : activeCampaignIds) {
                // Get multiplier
                connection.get(redisTemplate.getStringSerializer().serialize(String.format(RedisKeyConstants.PREFIX_AD_CAMPAIGN_PACING, campaignId)));
                // Get current daily spend
                connection.get(redisTemplate.getStringSerializer().serialize("campaign:spend:daily:" + campaignId));
            }
            return null;
        });
        
        // 2. Fetch daily budgets
        Map<String, CampaignPacingDTO> dailyBudgets = campaignClient.getDailyBudgets(activeCampaignIds);
        Map<String, Double> updatedMultipliers = new HashMap<>();
        
        // 3. Evaluate
        for (int i = 0; i < activeCampaignIds.size(); i++) {
            String campaignId = activeCampaignIds.get(i);
            
            Object currentSObj = pipelineResults.get(i * 2);
            Object spendObj = pipelineResults.get(i * 2 + 1);
            
            CampaignPacingDTO pacingDTO = dailyBudgets.get(campaignId);
            if (pacingDTO == null || pacingDTO.getDailyBudget() == null || pacingDTO.getDailyBudget() <= 0) {
                // Fail fast: Do NOT use a hardcoded default like 50.0. 
                // Exclude from bidding if we don't have a valid budget.
                updatedMultipliers.put(campaignId, 0.0);
                continue;
            }
            
            double currentSpend = parseDouble(spendObj, 0.0);
            double targetSpend = pacingDTO.getDailyBudget(); // In a real system, scaled by hours elapsed
            double currentS = parseMultiplier(currentSObj);
            
            if (currentSpend > targetSpend) {
                currentS = Math.max(S_MIN, currentS - 0.05);
            } else {
                currentS = Math.min(1.0, currentS + 0.05);
            }
            // Notification: Budget Running Low (<20% remaining)
            if (currentSpend >= 0.8 * pacingDTO.getDailyBudget()) {
                sendBudgetLowNotification(campaignId, pacingDTO.getAdvertiserId() != null ? pacingDTO.getAdvertiserId().toString() : campaignId);
            }
            updatedMultipliers.put(campaignId, currentS);
        }
        // 4. Pipeline Set all new multipliers
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, Double> entry : updatedMultipliers.entrySet()) {
                byte[] key = redisTemplate.getStringSerializer().serialize(String.format(RedisKeyConstants.PREFIX_AD_CAMPAIGN_PACING, entry.getKey()));
                byte[] value = redisTemplate.getStringSerializer().serialize(String.valueOf(entry.getValue()));
                connection.setEx(key, 3600, value); // TTL 1 hour
            }
            return null;
        });
    }

    private double parseMultiplier(Object val) {
        return parseDouble(val, 1.0);
    }

    private double parseDouble(Object val, double defaultVal) {
        if (val == null) return defaultVal;
        try {
            return Double.parseDouble(val.toString());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    private void sendBudgetLowNotification(String campaignId, String advertiserIdStr) {
        String notifiedKey = "pacing:budget_low_notified:" + campaignId;
        Boolean alreadyNotified = redisTemplate.hasKey(notifiedKey);
        if (Boolean.FALSE.equals(alreadyNotified)) {
            NotificationRequestEvent evt = NotificationRequestEvent.builder()
                .channel(ChannelType.EMAIL)
                .eventName("BUDGET_RUNNING_LOW")
                .explicitRecipient(advertiserIdStr)
                .payload(Map.of("campaignId", campaignId, "message", "Your campaign budget is running low (<20% remaining)."))
                .build();
            notificationRouterService.routeNotification(evt);
            redisTemplate.opsForValue().set(notifiedKey, "true", java.time.Duration.ofHours(24));
        }
    }
}
