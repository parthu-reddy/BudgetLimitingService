package com.fooddelivery.advertisement.pacing.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.common.constants.KafkaConstants;
import com.fooddelivery.common.constants.RedisKeyConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class CampaignSyncConsumer {
private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    public CampaignSyncConsumer(ObjectMapper objectMapper, StringRedisTemplate redisTemplate) {
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(topics = KafkaConstants.TOPIC_AD_EVENTS, groupId = "budget-pacing-service-sync")
    public void consumeCampaignEvent(String message) throws Exception {
        JsonNode root = objectMapper.readTree(message);

            if (root.has("eventType") && root.has("payload")) {
                String eventTypeStr = root.get("eventType").asText();
                JsonNode payload = root.get("payload");

                if (payload.isTextual()) {
                    payload = objectMapper.readTree(payload.asText());
                }

                String campaignId = payload.has("id") ? payload.get("id").asText() : null;
                if (campaignId == null) {
                    return;
                }

                if (EventType.AD_CAMPAIGN_CREATED.name().equals(eventTypeStr) ||
                    EventType.AD_CAMPAIGN_UPDATED.name().equals(eventTypeStr)) {

                    String maxBid = payload.has("maxBid") ? payload.get("maxBid").asText() : null;
                    String advertiserId = payload.has("advertiserId") ? payload.get("advertiserId").asText() : null;

                    Duration ttl = Duration.ofHours(48).plusMinutes(ThreadLocalRandom.current().nextLong(60));
                    if (maxBid != null) {
                        String bidKey = String.format(RedisKeyConstants.PREFIX_AD_CAMPAIGN_MAX_BID, campaignId);
                        redisTemplate.opsForValue().set(bidKey, maxBid, ttl);
                    }
                    if (advertiserId != null) {
                        String advKey = String.format(RedisKeyConstants.PREFIX_AD_CAMPAIGN_ADVERTISER, campaignId);
                        redisTemplate.opsForValue().set(advKey, advertiserId, ttl);
                    }
                    
                    redisTemplate.opsForSet().add(RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, campaignId);
                    log.info("Synced campaign {} to Redis (Created/Updated)", campaignId);

                } else if (EventType.AD_CAMPAIGN_PAUSED.name().equals(eventTypeStr) ||
                           EventType.AD_CAMPAIGN_DELETED.name().equals(eventTypeStr) ||
                           EventType.AD_CAMPAIGN_BUDGET_EXHAUSTED.name().equals(eventTypeStr)) {

                    redisTemplate.opsForSet().remove(RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, campaignId);
                    log.info("Removed campaign {} from active Redis set due to event {}", campaignId, eventTypeStr);
                }
            }
    }
}
