package com.fooddelivery.advertisement.pacing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.advertisement.pacing.client.CampaignClient;
import com.fooddelivery.common.dto.campaign.CampaignPacingDTO;
import com.fooddelivery.common.constants.RedisKeyConstants;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.common.service.NotificationRouterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PacingEngineServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;
    @Mock
    private CampaignClient campaignClient;
    @Mock
    private NotificationRouterService notificationRouterService;
    @Mock
    private OutboxEventRepository outboxEventRepository;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private SetOperations<String, String> setOperations;

    /* A real in-memory registry: a mocked MeterRegistry returns null from summary(),
       which NPEs when the pacing loop records pacing_multiplier. */
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry =
            new io.micrometer.core.instrument.simple.SimpleMeterRegistry();

    private PacingEngineService pacingEngineService;
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        // Noon UTC: exactly half of a UTC advertiser's day has passed.
        pacingEngineService = new PacingEngineService(
                redisTemplate, campaignClient, notificationRouterService, outboxEventRepository, objectMapper, meterRegistry,
                java.time.Clock.fixed(java.time.Instant.parse("2026-09-25T12:00:00Z"), java.time.ZoneOffset.UTC)
        );
        ReflectionTestUtils.setField(pacingEngineService, "timeOfDayTargetEnabled", true);
    }

    @Test
    void throttlesDownWhenAheadOfSchedule() {
        String campaignId = UUID.randomUUID().toString();
        List<String> activeCampaigns = Collections.singletonList(campaignId);

        // Current multiplier is 1.0, current spend is 60.00.
        // UserTrackingService stores spend as ten-thousandths (DECIMAL(19,4)), so 60.00 rupees
        // is 600000 in Redis. Passing "60.0" here would be read back as 0.006 and the
        // ahead-of-schedule branch would never be reached.
        List<Object> pipelineResults = List.of("1.0", "600000", "600000");
        when(redisTemplate.executePipelined(any(RedisCallback.class))).thenReturn(pipelineResults);

        CampaignPacingDTO dto = new CampaignPacingDTO();
        dto.setDailyBudget(100.0);
        dto.setAdvertiserId(UUID.randomUUID());
        dto.setTimeZone("UTC");
        when(campaignClient.getDailyBudgets(activeCampaigns)).thenReturn(Map.of(campaignId, dto));

        // Mock time to be 12:00:00 UTC (exactly 50% of the day)
        // Target spend = 100.0 * 0.5 = 50.0
        // Current spend = 60.0, which is ahead of schedule! (60 > 50)
        // Adjustment ratio = 50.0 / 60.0 = 0.833...
        // New multiplier should be 1.0 * 0.833 = 0.833 (which is less than 1.0)
        pacingEngineService.evaluatePacingForCampaigns(activeCampaigns);

        // Verify that the new multiplier was written via pipeline, and it's less than 1.0
        verify(redisTemplate, times(2)).executePipelined(any(RedisCallback.class));
        
        // Since executePipelined doesn't directly expose the setEx arguments in a standard mock verify, 
        // we can verify the outbox event for AD_CAMPAIGN_PACING_UPDATED has a decreased multiplier.
        verify(outboxEventRepository).save(argThat(entity -> {
            if (entity.getEventType() == com.fooddelivery.common.constants.EventType.AD_CAMPAIGN_PACING_UPDATED) {
                try {
                    com.fooddelivery.common.event.CampaignChangedEvent eventPayload = objectMapper.readValue(entity.getPayload(), com.fooddelivery.common.event.CampaignChangedEvent.class);
                    return eventPayload.getPacingMultiplier() < 1.0;
                } catch (Exception e) {
                    return false;
                }
            }
            return false;
        }));
    }

    /** Drive one evaluation where the campaign has spent its whole daily budget. */
    private com.fooddelivery.common.outbox.entity.OutboxEventEntity exhaust(String campaignId, double dailyBudget) {
        List<String> activeCampaigns = Collections.singletonList(campaignId);
        // Spend == budget, in ten-thousandths.
        String spend = String.valueOf((long) (dailyBudget * 10000));
        when(redisTemplate.executePipelined(any(RedisCallback.class))).thenReturn(List.of("1.0", spend, spend));
        when(redisTemplate.opsForSet()).thenReturn(setOperations);

        CampaignPacingDTO dto = new CampaignPacingDTO();
        dto.setDailyBudget(dailyBudget);
        dto.setAdvertiserId(UUID.randomUUID());
        dto.setTimeZone("UTC");
        when(campaignClient.getDailyBudgets(activeCampaigns)).thenReturn(Map.of(campaignId, dto));

        pacingEngineService.evaluatePacingForCampaigns(activeCampaigns);

        var captor = org.mockito.ArgumentCaptor.forClass(com.fooddelivery.common.outbox.entity.OutboxEventEntity.class);
        verify(outboxEventRepository, atLeastOnce()).save(captor.capture());
        return captor.getAllValues().stream()
                .filter(e -> e.getEventType() == com.fooddelivery.common.constants.EventType.AD_CAMPAIGN_BUDGET_EXHAUSTED)
                .reduce((a, b) -> b)
                .orElseThrow(() -> new AssertionError("no AD_CAMPAIGN_BUDGET_EXHAUSTED event was emitted"));
    }

    /**
     * The exhaustion event's idempotency key identifies an <em>episode</em>, not an instant.
     *
     * <p>It was {@code campaignId + ":exhausted:" + System.currentTimeMillis()} — unique by
     * construction, so `outbox_events.idempotency_key` (which is UNIQUE) enforced nothing and the
     * row merely looked protected. Re-running the same evaluation must now produce the same key.
     */
    @Test
    void theExhaustionKeyIsTheSameForTheSameEpisode() {
        String campaignId = UUID.randomUUID().toString();

        String first = exhaust(campaignId, 100.0).getIdempotencyKey();
        reset(outboxEventRepository);
        String second = exhaust(campaignId, 100.0).getIdempotencyKey();

        org.junit.jupiter.api.Assertions.assertEquals(first, second,
                "two emits of the same exhaustion must collide on the unique constraint");
        org.junit.jupiter.api.Assertions.assertTrue(first.contains(campaignId), first);
    }

    /**
     * A top-up is a new episode, so it must emit again.
     *
     * <p>This is why the key carries the budget and not just the day: keying on the day alone would
     * swallow the second exhaustion after a same-day top-up and leave the campaign serving with no
     * budget.
     */
    @Test
    void aToppedUpBudgetIsANewEpisode() {
        String campaignId = UUID.randomUUID().toString();

        String before = exhaust(campaignId, 100.0).getIdempotencyKey();
        reset(outboxEventRepository);
        String afterTopUp = exhaust(campaignId, 250.0).getIdempotencyKey();

        org.junit.jupiter.api.Assertions.assertNotEquals(before, afterTopUp,
                "exhausting a topped-up budget is a different fact and must not be deduplicated");
    }

    /**
     * The pacing-update event carries no idempotency key, deliberately.
     *
     * <p>It is a periodic sample of a continuously varying multiplier: there is no natural key for
     * "this sample", and the same multiplier recurs legitimately later in the day. A null key says
     * that honestly; the timestamp key it used to carry said the opposite.
     */
    @Test
    void thePacingUpdateEventClaimsNoIdempotencyKey() {
        String campaignId = UUID.randomUUID().toString();
        List<String> activeCampaigns = Collections.singletonList(campaignId);
        when(redisTemplate.executePipelined(any(RedisCallback.class))).thenReturn(List.of("1.0", "600000", "600000"));

        CampaignPacingDTO dto = new CampaignPacingDTO();
        dto.setDailyBudget(100.0);
        dto.setAdvertiserId(UUID.randomUUID());
        dto.setTimeZone("UTC");
        when(campaignClient.getDailyBudgets(activeCampaigns)).thenReturn(Map.of(campaignId, dto));

        pacingEngineService.evaluatePacingForCampaigns(activeCampaigns);

        var captor = org.mockito.ArgumentCaptor.forClass(com.fooddelivery.common.outbox.entity.OutboxEventEntity.class);
        verify(outboxEventRepository, atLeastOnce()).save(captor.capture());
        captor.getAllValues().stream()
                .filter(e -> e.getEventType() == com.fooddelivery.common.constants.EventType.AD_CAMPAIGN_PACING_UPDATED)
                .forEach(e -> org.junit.jupiter.api.Assertions.assertNull(e.getIdempotencyKey(),
                        "a periodic sample has no natural key; a fabricated one enforces nothing"));
    }

    /**
     * Defect D6 (TimezoneCorrectness_2026-09-25): today's spend is read from the key for today on the
     * advertiser's calendar. At 20:45Z it is already the 26th in Kolkata and still the 25th in New York.
     * The old code read one rolling 24h key for every campaign.
     */
    @Test
    void readsTodaysSpendKeyOnEachAdvertisersCalendar() throws Exception {
        PacingEngineService lateEvening = new PacingEngineService(
                redisTemplate, campaignClient, notificationRouterService, outboxEventRepository, objectMapper, meterRegistry,
                java.time.Clock.fixed(java.time.Instant.parse("2026-09-25T20:45:00Z"), java.time.ZoneOffset.UTC));
        String kolkataCampaign = UUID.randomUUID().toString();
        String newYorkCampaign = UUID.randomUUID().toString();
        List<String> active = List.of(kolkataCampaign, newYorkCampaign);
        when(campaignClient.getDailyBudgets(active)).thenReturn(Map.of(
                kolkataCampaign, new CampaignPacingDTO(100.0, null, UUID.randomUUID(), "Asia/Kolkata"),
                newYorkCampaign, new CampaignPacingDTO(100.0, null, UUID.randomUUID(), "America/New_York")));
        when(redisTemplate.getStringSerializer()).thenReturn((org.springframework.data.redis.serializer.RedisSerializer) org.springframework.data.redis.serializer.RedisSerializer.string());
        List<String> keysRead = new java.util.ArrayList<>();
        when(redisTemplate.executePipelined(any(RedisCallback.class))).thenAnswer(inv -> {
            org.springframework.data.redis.connection.RedisConnection connection = org.mockito.Mockito.mock(org.springframework.data.redis.connection.RedisConnection.class);
            org.mockito.Mockito.lenient().when(connection.get(any(byte[].class))).thenAnswer(g -> {
                keysRead.add(new String((byte[]) g.getArgument(0), java.nio.charset.StandardCharsets.UTF_8));
                return null;
            });
            ((RedisCallback<?>) inv.getArgument(0)).doInRedis(connection);
            return keysRead.isEmpty() ? List.of() : java.util.Collections.nCopies(keysRead.size(), (Object) null);
        });

        lateEvening.evaluatePacingForCampaigns(active);

        org.junit.jupiter.api.Assertions.assertTrue(keysRead.contains("campaign:spend:daily:" + kolkataCampaign + ":2026-09-26"), keysRead.toString());
        org.junit.jupiter.api.Assertions.assertTrue(keysRead.contains("campaign:spend:daily:" + newYorkCampaign + ":2026-09-25"), keysRead.toString());
    }
}
