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

        private final com.fooddelivery.common.event.EventBinder eventBinder;

public CampaignSyncConsumer(ObjectMapper objectMapper, StringRedisTemplate redisTemplate, IIdempotencyKeyRepository idempotencyKeyRepository, TransactionTemplate transactionTemplate, @org.springframework.context.annotation.Lazy com.fooddelivery.advertisement.pacing.service.PacingEngineService pacingEngineService, MeterRegistry meterRegistry, com.fooddelivery.common.event.EventBinder eventBinder) {
        this.eventBinder = eventBinder;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.transactionTemplate = transactionTemplate;
        this.pacingEngineService = pacingEngineService;
        this.meterRegistry = meterRegistry;
    }

    @RetryableTopic(attempts = "5", backoff = @Backoff(delay = 1000, multiplier = 2.0), autoCreateTopics = "true", dltStrategy = DltStrategy.FAIL_ON_ERROR, exclude = {com.fooddelivery.common.event.EventBindingException.class}, traversingCauses = "true")
    @KafkaListener(topics = KafkaConstants.TOPIC_AD_EVENTS, groupId = "budget-pacing-service-sync-campaignsyncconsumer")
    public void consumeCampaignEvent(String message, @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) throws Exception {
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

            // ad-events carries the type in a Kafka header and CampaignChangedEvent flat in the
            // body, so this binds; see BiddingEngine.CampaignEventConsumer for the same shape.
            String eventTypeStr = com.fooddelivery.common.util.KafkaHeaderUtils.extractEventType(headers, null);
            if (eventTypeStr == null) {
                log.warn("Dropping ad-event with no eventType header");
                return null;
            }
            final EventType eventType;
            try {
                eventType = EventType.valueOf(eventTypeStr);
            } catch (IllegalArgumentException e) {
                log.info("Unknown event type {} on ad-events. Ignoring.", eventTypeStr);
                return null;
            }
            // AD_CREATIVE_* also rides this topic with a raw AdCreative payload -- a different
            // shape entirely. Return before binding rather than producing an object of nulls.
            if (!HANDLED_EVENT_TYPES.contains(eventType)) {
                log.debug("Event {} not handled by pacing sync. Ignoring.", eventTypeStr);
                return null;
            }

            com.fooddelivery.common.event.CampaignChangedEvent event =
                    eventBinder.bindIf(eventType, eventTypeStr, message,
                            com.fooddelivery.common.event.CampaignChangedEvent.class)
                    .orElseThrow(() -> new IllegalStateException(
                            "bindIf returned empty for " + eventTypeStr
                                    + " despite an exact event-type match"));

            if (event.getCampaignId() == null) {
                meterRegistry.counter("campaign_event_dropped_total", "reason", "no_campaign_id").increment();
                log.warn("Dropping ad-event with no resolvable campaign id: {}", eventTypeStr);
                return null;
            }
            String campaignId = event.getCampaignId().toString();

            if (eventType == EventType.AD_CAMPAIGN_CREATED || eventType == EventType.AD_CAMPAIGN_UPDATED
                    || eventType == EventType.AD_CAMPAIGN_RESUMED) {
                // maxBid stays a String on the way into Redis, but it is a BigDecimal on the event
                // now -- so the value written is the exact decimal, not whatever asText() produced.
                String maxBid = event.getMaxBid() != null ? event.getMaxBid().toPlainString() : null;
                String advertiserId = event.getAdvertiserId() != null ? event.getAdvertiserId().toString() : null;
                String campaignStatus = event.getStatus() != null ? event.getStatus() : "ACTIVE";

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

                    if (eventType == EventType.AD_CAMPAIGN_UPDATED || eventType == EventType.AD_CAMPAIGN_RESUMED) {
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
            } else {
                redisTemplate.opsForSet().remove(RedisKeyConstants.KEY_ACTIVE_CAMPAIGNS, campaignId);
                log.info("Removed campaign {} from active Redis set due to event {}", campaignId, eventTypeStr);
            }
            return null;
        });
    }

    /** The campaign events pacing sync acts on; ad-events also carries AD_CREATIVE_* and others. */
    private static final java.util.Set<EventType> HANDLED_EVENT_TYPES = java.util.EnumSet.of(
            EventType.AD_CAMPAIGN_CREATED, EventType.AD_CAMPAIGN_UPDATED, EventType.AD_CAMPAIGN_RESUMED,
            EventType.AD_CAMPAIGN_PAUSED, EventType.AD_CAMPAIGN_DELETED, EventType.AD_CAMPAIGN_COMPLETED,
            EventType.AD_CAMPAIGN_BUDGET_EXHAUSTED);

    @DltHandler
    public void handleDlt(Object message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("Campaign event failed all retries and sent to DLT: {} - {}", topic, message);
        meterRegistry.counter("kafka_dlt_depth_total", "topic", topic).increment();
    }
}
