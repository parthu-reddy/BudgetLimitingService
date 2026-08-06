package com.fooddelivery.advertisement.pacing.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.stereotype.Service;
import com.fooddelivery.common.constants.RedisKeyConstants;
import com.fooddelivery.advertisement.pacing.client.LedgerClient;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import com.fooddelivery.common.service.NotificationRouterService;
import com.fooddelivery.common.event.NotificationRequestEvent;
import com.fooddelivery.common.enums.ChannelType;

@Service
public class PacingEngineService {

    private final StringRedisTemplate redisTemplate;
    private final LedgerClient ledgerClient;
    private final NotificationRouterService notificationRouterService;
    
    // Lower bound floor to prevent 0% win rate
    private static final double S_MIN = 0.1;
    
    public PacingEngineService(StringRedisTemplate redisTemplate, LedgerClient ledgerClient, NotificationRouterService notificationRouterService) {
        this.redisTemplate = redisTemplate;
        this.ledgerClient = ledgerClient;
        this.notificationRouterService = notificationRouterService;
    }
    
    public void evaluatePacingForCampaigns(List<String> activeCampaignIds) {
        if (activeCampaignIds == null || activeCampaignIds.isEmpty()) return;
        
        // 1. Pipeline Get all current multipliers
        List<Object> currentMultipliers = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String campaignId : activeCampaignIds) {
                connection.get(redisTemplate.getStringSerializer().serialize(
                    String.format(RedisKeyConstants.PREFIX_AD_CAMPAIGN_PACING, campaignId)
                ));
            }
            return null;
        });

        // 2. Fetch bulk spends and budgets
        Map<String, Double> currentSpends = ledgerClient.getCurrentSpendBatch(activeCampaignIds);
        Map<String, Double> dailyBudgets = ledgerClient.getDailyBudgets(activeCampaignIds);

        Map<String, Double> updatedMultipliers = new HashMap<>();
        
        // 3. Evaluate
        for (int i = 0; i < activeCampaignIds.size(); i++) {
            String campaignId = activeCampaignIds.get(i);
            
            Double spendObj = currentSpends.get(campaignId);
            Double budgetObj = dailyBudgets.get(campaignId);
            
            if (budgetObj == null || budgetObj <= 0) {
                // Fail fast: Do NOT use a hardcoded default like 50.0. 
                // Exclude from bidding if we don't have a valid budget.
                updatedMultipliers.put(campaignId, 0.0);
                continue;
            }
            
            double currentSpend = spendObj != null ? spendObj : 0.0;
            double targetSpend = budgetObj; // In a real system, scaled by hours elapsed
            
            double currentS = parseMultiplier(currentMultipliers.get(i));
            
            if (currentSpend > targetSpend) {
                currentS = Math.max(S_MIN, currentS - 0.05);
            } else {
                currentS = Math.min(1.0, currentS + 0.05);
            }
            
            // Notification: Budget Running Low (<20% remaining)
            if (currentSpend >= 0.8 * budgetObj) {
                sendBudgetLowNotification(campaignId);
            }
            
            updatedMultipliers.put(campaignId, currentS);
        }
        
        // 4. Pipeline Set all new multipliers
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, Double> entry : updatedMultipliers.entrySet()) {
                byte[] key = redisTemplate.getStringSerializer().serialize(
                    String.format(RedisKeyConstants.PREFIX_AD_CAMPAIGN_PACING, entry.getKey())
                );
                byte[] value = redisTemplate.getStringSerializer().serialize(String.valueOf(entry.getValue()));
                connection.setEx(key, 3600, value); // TTL 1 hour
            }
            return null;
        });
    }
    
    private double parseMultiplier(Object val) {
        if (val == null) return 1.0;
        try {
            return Double.parseDouble(val.toString());
        } catch (NumberFormatException e) {
            return 1.0;
        }
    }
    
    private void sendBudgetLowNotification(String campaignId) {
        String notifiedKey = "pacing:budget_low_notified:" + campaignId;
        Boolean alreadyNotified = redisTemplate.hasKey(notifiedKey);
        
        if (Boolean.FALSE.equals(alreadyNotified)) {
            NotificationRequestEvent evt = NotificationRequestEvent.builder()
                .channel(ChannelType.EMAIL) // Or IN_APP
                .eventName("BUDGET_RUNNING_LOW")
                .explicitRecipient(campaignId) // Since we don't have advertiserId here directly
                .payload(Map.of("campaignId", campaignId, "message", "Your campaign budget is running low (<20% remaining)."))
                .build();
                
            notificationRouterService.routeNotification(evt);
            redisTemplate.opsForValue().set(notifiedKey, "true", java.time.Duration.ofHours(24));
        }
    }
}
