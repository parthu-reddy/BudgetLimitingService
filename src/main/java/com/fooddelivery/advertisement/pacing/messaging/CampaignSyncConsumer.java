package com.fooddelivery.advertisement.pacing.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.common.constants.KafkaConstants;
import com.fooddelivery.common.constants.RedisKeyConstants;
import com.fooddelivery.common.entity.IdempotencyKey;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.UUID;

@Service
@lombok.extern.slf4j.Slf4j
public class CampaignSyncConsumer {

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    private final IIdempotencyKeyRepository idempotencyKeyRepository;
    private final TransactionTemplate transactionTemplate;

    public CampaignSyncConsumer(ObjectMapper objectMapper, StringRedisTemplate redisTemplate, IIdempotencyKeyRepository idempotencyKeyRepository, TransactionTemplate transactionTemplate) {
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.transactionTemplate = transactionTemplate;
    }

    @RetryableTopic(attempts = "5", backoff = @Backoff(delay = 1000, multiplier = 2.0), autoCreateTopics = "true", dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = KafkaConstants.TOPIC_AD_EVENTS, groupId = "budget-pacing-service-sync")
    public void consumeCampaignEvent(String message, @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) throws Exception {
        JsonNode root = objectMapper.readTree(message);
        
        String extractedEventId = com.fooddelivery.common.util.KafkaHeaderUtils.extractHeaderValue(headers, "eventId");
        final String resolvedEventId;
        if (extractedEventId == null) {
            resolvedEventId = UUID.nameUUIDFromBytes(message.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
        } else {
            resolvedEventId = extractedEventId;
        }

        String idempotencyKeyStr = "processed_event:" + resolvedEventId;

        transactionTemplate.execute(status -> {
            if (idempotencyKeyRepository.existsById(idempotencyKeyStr)) {
                log.info("Duplicate campaign sync event ignored: {}", idempotencyKeyStr);
                return null;
            }
            idempotencyKeyRepository.save(new IdempotencyKey(idempotencyKeyStr));

            // See BiddingEngine.CampaignEventConsumer: ad-events carries a FLAT Campaign, so the
            // previous {eventType, payload} guard never fired and pacing data was never synced.
            String eventTypeStr = com.fooddelivery.common.util.EventPayloadUtils.resolveEventType(root, headers);
            JsonNode payload = com.fooddelivery.common.util.EventPayloadUtils.unwrapPayload(root);
            if (eventTypeStr != null && payload != null) {
                String campaignId = payload.has("id") ? payload.get("id").asText() : null;
                if (campaignId == null) {
                    return null;
                }
                if (EventType.AD_CAMPAIGN_CREATED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_UPDATED.name().equals(eventTypeStr)) {
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
                } else if (EventType.AD_CAMPAIGN_PAUSED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_DELETED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_BUDGET_EXHAUSTED.name().equals(eventTypeStr)) {
                    redisTemplate.opsForSet().remove(RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, campaignId);
                    log.info("Removed campaign {} from active Redis set due to event {}", campaignId, eventTypeStr);
                }
            }
            return null;
        });
    }

    @DltHandler
    public void handleDlt(Object message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.err.println("Message failed 5 times and sent to DLT: " + topic + " - " + message);
    }
}
