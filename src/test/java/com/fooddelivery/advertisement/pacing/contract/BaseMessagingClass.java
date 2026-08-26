package com.fooddelivery.advertisement.pacing.contract;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base class for the generated producer contract tests in {@code contracts/messaging}.
 *
 * <p>BudgetLimitingService owns the two pacing events that BiddingEngine consumes:
 * AD_CAMPAIGN_BUDGET_EXHAUSTED and AD_CAMPAIGN_PACING_UPDATED. Both are published through the
 * shared OutboxProcessor (ADVERTISEMENT -&gt; ad-events), so the triggers below drive the real
 * processor rather than hand-rolling a Kafka send.
 */
@SpringBootTest(classes = BaseMessagingClass.TestConfig.class, webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration"})
@org.springframework.test.context.ActiveProfiles("contract-test")
@AutoConfigureMessageVerifier
@EmbeddedKafka(partitions = 1, topics = {"ad-events"})
public abstract class BaseMessagingClass {

    @org.springframework.boot.SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    static class TestConfig {
        @Bean
        public KafkaMessageVerifier kafkaMessageVerifier() {
            return new KafkaMessageVerifier();
        }
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers", "localhost:9092"));
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    public void fireBudgetExhaustedEvent() throws Exception {
        fireEvent(com.fooddelivery.common.constants.EventType.AD_CAMPAIGN_BUDGET_EXHAUSTED, 1.0, true);
    }

    public void firePacingUpdatedEvent() throws Exception {
        fireEvent(com.fooddelivery.common.constants.EventType.AD_CAMPAIGN_PACING_UPDATED, 0.85, false);
    }

    private void fireEvent(com.fooddelivery.common.constants.EventType eventType,
                           double pacingMultiplier,
                           boolean budgetExhausted) throws Exception {
        com.fooddelivery.common.event.CampaignChangedEvent event =
            com.fooddelivery.common.event.CampaignChangedEvent.builder()
                .campaignId(java.util.UUID.fromString("1d9c4f70-2a83-4b16-9e5d-7c0a3b8f6e41"))
                .advertiserId(java.util.UUID.fromString("3e14926d-0c98-5840-abcd-37ec439ddc25"))
                .status("ACTIVE")
                .maxBid(new java.math.BigDecimal("12.50"))
                .budget(new java.math.BigDecimal("500.00"))
                .budgetExhausted(budgetExhausted)
                .pacingMultiplier(pacingMultiplier)
                .schemaVersion(2)
                .build();

        com.fooddelivery.common.outbox.entity.OutboxEventEntity outboxEvent =
                com.fooddelivery.common.outbox.entity.OutboxEventEntity.builder()
                        .id(java.util.UUID.randomUUID())
                        .aggregateType(com.fooddelivery.common.constants.AggregateType.ADVERTISEMENT)
                        .aggregateId(event.getCampaignId().toString())
                        .eventType(eventType)
                        .payload(objectMapper.writeValueAsString(event))
                        .createdAt(java.time.LocalDateTime.now())
                        .build();

        com.fooddelivery.common.outbox.repository.OutboxEventRepository repo =
                org.mockito.Mockito.mock(com.fooddelivery.common.outbox.repository.OutboxEventRepository.class);
        org.mockito.Mockito.when(repo.findTop100ByStatusInOrderByCreatedAtAsc(org.mockito.ArgumentMatchers.anyList()))
                .thenReturn(new java.util.ArrayList<>(java.util.List.of(outboxEvent)));
        new com.fooddelivery.common.outbox.service.OutboxProcessor(
                repo, kafkaTemplate, new io.micrometer.core.instrument.simple.SimpleMeterRegistry())
            .processOutboxEvents();
    }
}
