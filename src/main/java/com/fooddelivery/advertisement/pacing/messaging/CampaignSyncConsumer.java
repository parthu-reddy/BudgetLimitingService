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
import io.micrometer.core.instrument.MeterRegistry;

@Service
@lombok.extern.slf4j.Slf4j
public class CampaignSyncConsumer {

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    private final IIdempotencyKeyRepository idempotencyKeyRepository;
    private final TransactionTemplate transactionTemplate;
    private final com.fooddelivery.advertisement.pacing.service.PacingEngineService pacingEngineService;
    private final MeterRegistry meterRegistry;

    public CampaignSyncConsumer(ObjectMapper objectMapper, StringRedisTemplate redisTemplate, IIdempotencyKeyRepository idempotencyKeyRepository, TransactionTemplate transactionTemplate, @org.springframework.context.annotation.Lazy com.fooddelivery.advertisement.pacing.service.PacingEngineService pacingEngineService, MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.transactionTemplate = transactionTemplate;
        this.pacingEngineService = pacingEngineService;
        this.meterRegistry = meterRegistry;
    }

    @RetryableTopic(attempts = "5", backoff = @Backoff(delay = 1000, multiplier = 2.0), autoCreateTopics = "true", dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = KafkaConstants.TOPIC_AD_EVENTS, groupId = "budget-pacing-service-sync-campaignsyncconsumer")
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
            if (idempotencyKeyRepository.tryClaim(idempotencyKeyStr) == 0) {
                log.info("Duplicate campaign sync event ignored: {}", idempotencyKeyStr);
                return null;
            }

            // See BiddingEngine.CampaignEventConsumer: ad-events carries a FLAT Campaign, so the
            // previous {eventType, payload} guard never fired and pacing data was never synced.
            String eventTypeStr = com.fooddelivery.common.util.EventPayloadUtils.resolveEventType(root, headers);
            JsonNode payload = root;
            if (eventTypeStr != null && payload != null) {
                String campaignId = com.fooddelivery.common.util.EventPayloadUtils.campaignId(payload);
                if (campaignId == null) {
                    meterRegistry.counter("campaign_event_dropped_total", "reason", "no_campaign_id").increment();
                    log.warn("Dropping ad-event with no resolvable campaign id: {}", eventTypeStr);
                    return null;
                }
                if (EventType.AD_CAMPAIGN_CREATED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_UPDATED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_RESUMED.name().equals(eventTypeStr)) {
                    String maxBid = payload.has("maxBid") ? payload.get("maxBid").asText() : null;
                    String advertiserId = payload.has("advertiserId") ? payload.get("advertiserId").asText() : null;
                    String campaignStatus = payload.has("status") ? payload.get("status").asText() : "ACTIVE";
                    
                    if ("ACTIVE".equals(campaignStatus)) {
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
                        log.info("Synced campaign {} to Redis (Created/Updated/Resumed and ACTIVE)", campaignId);
                        
                        if (EventType.AD_CAMPAIGN_UPDATED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_RESUMED.name().equals(eventTypeStr)) {
                            // Synchronously evaluate pacing to prevent overspend if the new budget is still exhausted
                            // and to emit PACING_UPDATED if it was replenished.
                            try {
                                pacingEngineService.evaluatePacingForCampaigns(java.util.Collections.singletonList(campaignId));
                            } catch (Exception e) {
                                log.error("Failed to evaluate pacing synchronously for updated/resumed campaign {}", campaignId, e);
                            }
                        }
                    } else {
                        redisTemplate.opsForSet().remove(RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, campaignId);
                        log.info("Removed campaign {} from active Redis set due to event {} with status {}", campaignId, eventTypeStr, campaignStatus);
                    }
                } else if (EventType.AD_CAMPAIGN_PAUSED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_DELETED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_COMPLETED.name().equals(eventTypeStr) || EventType.AD_CAMPAIGN_BUDGET_EXHAUSTED.name().equals(eventTypeStr)) {
                    redisTemplate.opsForSet().remove(RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, campaignId);
                    log.info("Removed campaign {} from active Redis set due to event {}", campaignId, eventTypeStr);
                }
            }
            return null;
        });
    }

    @DltHandler
    public void handleDlt(Object message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("Campaign event failed all retries and sent to DLT: {} - {}", topic, message);
        meterRegistry.counter("kafka_dlt_depth_total", "topic", topic).increment();
    }
}
