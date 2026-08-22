package com.fooddelivery.advertisement.pacing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.advertisement.pacing.client.CampaignClient;
import com.fooddelivery.advertisement.pacing.dto.CampaignPacingDTO;
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
        pacingEngineService = new PacingEngineService(
                redisTemplate, campaignClient, notificationRouterService, outboxEventRepository, objectMapper, meterRegistry
        );
        ReflectionTestUtils.setField(pacingEngineService, "timeOfDayTargetEnabled", true);
        // businessZone is @Value-injected; a plain Mockito context leaves it null and
        // ZoneId.of(null) throws. The time mock below is built on UTC, so match it.
        ReflectionTestUtils.setField(pacingEngineService, "businessZone", "UTC");
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
        when(campaignClient.getDailyBudgets(activeCampaigns)).thenReturn(Map.of(campaignId, dto));

        // Mock time to be 12:00:00 UTC (exactly 50% of the day)
        // Target spend = 100.0 * 0.5 = 50.0
        // Current spend = 60.0, which is ahead of schedule! (60 > 50)
        // Adjustment ratio = 50.0 / 60.0 = 0.833...
        // New multiplier should be 1.0 * 0.833 = 0.833 (which is less than 1.0)
        java.time.LocalTime noon = java.time.LocalTime.of(12, 0, 0);
        try (var mockedTime = mockStatic(java.time.LocalTime.class)) {
            mockedTime.when(() -> java.time.LocalTime.now(java.time.ZoneId.of("UTC"))).thenReturn(noon);

            pacingEngineService.evaluatePacingForCampaigns(activeCampaigns);
        }

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
}
