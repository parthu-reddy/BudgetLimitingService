package com.fooddelivery.advertisement.pacing.contract;

import com.fooddelivery.common.contract.KafkaStubMessageSender;

import com.fooddelivery.advertisement.pacing.messaging.CampaignSyncConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.contract.stubrunner.StubTrigger;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;

/**
 * Consumes CampaignService's real ad_events stub and asserts the campaign is synced to Redis for
 * budget pacing.
 *
 * Before the EventPayloadUtils fix this consumer was guarded by
 * {@code root.has("eventType") && root.has("payload")}, never true against the flat Campaign that
 * CampaignService publishes, so pacing had no data at all.
 */
@SpringBootTest(classes = CampaignSyncConsumerContractTest.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration")
@ActiveProfiles("contract-test")
@AutoConfigureStubRunner(ids = "com.fooddelivery:campaign-service:+:stubs",
        stubsMode = StubRunnerProperties.StubsMode.LOCAL)
@EmbeddedKafka(partitions = 1, topics = {"ad-events"})
class CampaignSyncConsumerContractTest {

    @org.springframework.boot.SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    
    @Import(CampaignSyncConsumer.class)
    static class TestConfig {
        @Bean
        public MessageVerifierSender<Message<?>> kafkaStubMessageSender(KafkaTemplate<String, String> t) {
            return new KafkaStubMessageSender(t);
        }

        @Bean
        public TransactionTemplate transactionTemplate() {
            PlatformTransactionManager tm = Mockito.mock(PlatformTransactionManager.class);
            Mockito.when(tm.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
            return new TransactionTemplate(tm);
        }
    }

    @MockBean
    private StringRedisTemplate redisTemplate;

    @MockBean
    private com.fooddelivery.common.repository.IIdempotencyKeyRepository idempotencyKeyRepository;

    @Test
    void syncsTheCampaignToRedisForPacing() {
        ValueOperations<String, String> valueOps = Mockito.mock(ValueOperations.class);
        SetOperations<String, String> setOps = Mockito.mock(SetOperations.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOps);
        Mockito.when(redisTemplate.opsForSet()).thenReturn(setOps);

        // tryClaim is INSERT ... ON CONFLICT DO NOTHING: 1 = claimed, 0 = already seen.
        // An unstubbed int mock returns 0, so without this every event looks like a
        // duplicate and the consumer returns before touching Redis.
        Mockito.when(idempotencyKeyRepository.tryClaim(anyString())).thenReturn(1);

        stubTrigger.trigger("ad_events");

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            // The contract's campaign id is a regex matcher, so the stub generates a fresh UUID --
            // assert the interactions, not a literal id.
            verify(valueOps, Mockito.atLeastOnce())
                    .set(anyString(), anyString(), any(java.time.Duration.class));
            verify(setOps).add(anyString(), anyString());
        });
    }

    @Autowired
    private StubTrigger stubTrigger;
}
