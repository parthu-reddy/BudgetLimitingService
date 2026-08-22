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
import org.springframework.kafka.core.KafkaTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import com.fooddelivery.common.enums.OutboxStatus;
import com.fooddelivery.common.constants.AggregateType;
import com.fooddelivery.common.event.CampaignChangedEvent;

@Service
@lombok.extern.slf4j.Slf4j
public class PacingEngineService {
    @java.lang.SuppressWarnings("all")

    private final StringRedisTemplate redisTemplate;
    private final CampaignClient campaignClient;
    private final NotificationRouterService notificationRouterService;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;
    // Lower bound floor to prevent 0% win rate
    private static final double S_MIN = 0.1;

    @org.springframework.beans.factory.annotation.Value("${pacing.time-of-day-target.enabled:false}")
    private boolean timeOfDayTargetEnabled;

    @org.springframework.beans.factory.annotation.Value("${platform.business-zone:UTC}")
    private String businessZone;

    public PacingEngineService(StringRedisTemplate redisTemplate, CampaignClient campaignClient, NotificationRouterService notificationRouterService, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper, io.micrometer.core.instrument.MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.campaignClient = campaignClient;
        this.notificationRouterService = notificationRouterService;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    public void evaluatePacingForCampaigns(List<String> activeCampaignIds) {
        if (activeCampaignIds == null || activeCampaignIds.isEmpty()) return;
        // 1. Pipeline Get all current multipliers, daily spends, and lifetime spends
        List<Object> pipelineResults = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String campaignId : activeCampaignIds) {
                // Get multiplier
                connection.get(redisTemplate.getStringSerializer().serialize(String.format(RedisKeyConstants.PREFIX_AD_CAMPAIGN_PACING, campaignId)));
                // Get current daily spend
                connection.get(redisTemplate.getStringSerializer().serialize("campaign:spend:daily:" + campaignId));
                // Get current lifetime spend
                connection.get(redisTemplate.getStringSerializer().serialize("campaign:spend:lifetime:" + campaignId));
            }
            return null;
        });
        
        // 2. Fetch daily budgets
        Map<String, CampaignPacingDTO> dailyBudgets = campaignClient.getDailyBudgets(activeCampaignIds);
        Map<String, Double> updatedMultipliers = new HashMap<>();
        
        // 3. Evaluate
        for (int i = 0; i < activeCampaignIds.size(); i++) {
            String campaignId = activeCampaignIds.get(i);
            
            Object currentSObj = pipelineResults.get(i * 3);
            Object spendObj = pipelineResults.get(i * 3 + 1);
            Object lifetimeSpendObj = pipelineResults.get(i * 3 + 2);
            
            CampaignPacingDTO pacingDTO = dailyBudgets.get(campaignId);
            if (pacingDTO == null || pacingDTO.getDailyBudget() == null || pacingDTO.getDailyBudget() <= 0) {
                // Fail fast: Do NOT use a hardcoded default like 50.0. 
                // Exclude from bidding if we don't have a valid budget.
                updatedMultipliers.put(campaignId, 0.0);
                meterRegistry.summary("pacing_multiplier").record(0.0);
                continue;
            }
            
            double currentSpend = parseDouble(spendObj, 0.0) / 10000.0;
            double dailyBudget = pacingDTO.getDailyBudget();
            double targetSpend = dailyBudget;
            
            if (timeOfDayTargetEnabled) {
                java.time.LocalTime now = java.time.LocalTime.now(java.time.ZoneId.of(businessZone));
                double elapsedFractionOfDay = (now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond()) / 86400.0;
                targetSpend = dailyBudget * elapsedFractionOfDay;
            }
            
            double lifetimeSpend = parseDouble(lifetimeSpendObj, 0.0) / 10000.0;
            Double targetLifetimeSpend = pacingDTO.getLifetimeBudget();
            
            double currentS = parseMultiplier(currentSObj);
            double previousS = currentS;

            if (currentSpend >= dailyBudget || (targetLifetimeSpend != null && lifetimeSpend >= targetLifetimeSpend)) {
                // Budget exhausted
                updatedMultipliers.put(campaignId, 0.0);
                meterRegistry.summary("pacing_multiplier").record(0.0);
                emitBudgetExhaustedEvent(campaignId, pacingDTO.getAdvertiserId());
                // Remove from active campaigns set
                redisTemplate.opsForSet().remove(RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, campaignId);
                continue;
            }
            
            if (currentSpend == 0.0) {
                currentS = Math.min(1.0, currentS + 0.1); // Ramp up if no spend
            } else if (targetSpend > 0) {
                double adjustmentRatio = targetSpend / currentSpend;
                // Proportional adjustment with some dampening
                adjustmentRatio = Math.max(0.5, Math.min(1.5, adjustmentRatio));
                currentS = currentS * adjustmentRatio;
            }
            
            currentS = Math.max(S_MIN, Math.min(1.0, currentS));
            // Notification: Budget Running Low (<20% remaining)
            if (currentSpend >= 0.8 * pacingDTO.getDailyBudget()) {
                sendBudgetLowNotification(campaignId, pacingDTO.getAdvertiserId() != null ? pacingDTO.getAdvertiserId().toString() : campaignId);
            }
            updatedMultipliers.put(campaignId, currentS);
            meterRegistry.summary("pacing_multiplier").record(currentS);
            
            // Emit update if pacing changed, or if it was previously exhausted (pacing was 0.0)
            if (currentS != previousS || previousS == 0.0) {
                emitPacingUpdatedEvent(campaignId, pacingDTO.getAdvertiserId(), currentS, false);
            }
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
                .userId(advertiserIdStr != null ? UUID.fromString(advertiserIdStr) : null)
                .payload(Map.of("campaignId", campaignId, "message", "Your campaign budget is running low (<20% remaining)."))
                .build();
            notificationRouterService.routeNotification(evt);
            redisTemplate.opsForValue().set(notifiedKey, "true", java.time.Duration.ofHours(24));
        }
    }

    private void emitBudgetExhaustedEvent(String campaignId, UUID advertiserId) {
        try {
            CampaignChangedEvent eventPayload = CampaignChangedEvent.builder()
                .campaignId(UUID.fromString(campaignId))
                .advertiserId(advertiserId)
                .budgetExhausted(true)
                .build();
            
            OutboxEventEntity event = OutboxEventEntity.builder()
                .id(UUID.randomUUID())
                .createdAt(java.time.LocalDateTime.now())
                .aggregateType(AggregateType.ADVERTISEMENT)
                .aggregateId(campaignId)
                .eventType(EventType.AD_CAMPAIGN_BUDGET_EXHAUSTED)
                .idempotencyKey(campaignId + ":exhausted:" + System.currentTimeMillis())
                .payload(objectMapper.writeValueAsString(eventPayload))
                .status(OutboxStatus.UNPROCESSED)
                .build();
            
            outboxEventRepository.save(event);
        } catch (Exception e) {
            log.error("Failed to emit AD_CAMPAIGN_BUDGET_EXHAUSTED for campaign {}", campaignId, e);
        }
    }
    private void emitPacingUpdatedEvent(String campaignId, UUID advertiserId, double pacingMultiplier, boolean budgetExhausted) {
        try {
            CampaignChangedEvent eventPayload = CampaignChangedEvent.builder()
                .campaignId(UUID.fromString(campaignId))
                .advertiserId(advertiserId)
                .pacingMultiplier(pacingMultiplier)
                .budgetExhausted(budgetExhausted)
                .build();
            
            OutboxEventEntity event = OutboxEventEntity.builder()
                .id(UUID.randomUUID())
                .createdAt(java.time.LocalDateTime.now())
                .aggregateType(AggregateType.ADVERTISEMENT)
                .aggregateId(campaignId)
                .eventType(EventType.AD_CAMPAIGN_PACING_UPDATED)
                .idempotencyKey(campaignId + ":pacing:" + System.currentTimeMillis())
                .payload(objectMapper.writeValueAsString(eventPayload))
                .status(OutboxStatus.UNPROCESSED)
                .build();
            
            outboxEventRepository.save(event);
        } catch (Exception e) {
            log.error("Failed to emit AD_CAMPAIGN_PACING_UPDATED for campaign {}", campaignId, e);
        }
    }
}
