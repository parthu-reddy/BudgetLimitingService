package com.fooddelivery.advertisement.pacing;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The real application context starts.
 *
 * <p>Added 2026-09-13. This service had no full-context test, so the CommonLibrary split was verified
 * here by reading poms rather than by watching Spring start. That matters because
 * {@code AutoConfiguration.imports} is resolved at startup and Spring Boot aborts on an entry it
 * cannot load: a compiler never sees it, and neither does a unit test that avoids a context.
 *
 * <p>Uses {@code contract-test}, the profile already configured to boot without external
 * infrastructure — H2 for Postgres, the config server off. Beans that need a real server are mocked
 * individually rather than by excluding auto-configuration, so everything common-library contributes
 * is still created for real.
 */
@SpringBootTest
@ActiveProfiles("contract-test")
class ContextLoadTest {

    // The contract-test profile excludes Redis auto-configuration, so nothing supplies these.
    // Mocked individually rather than by excluding more auto-configuration: the point of this test is
    // that everything else -- including every bean common-library contributes -- is created for real.
    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.data.redis.core.StringRedisTemplate stringRedisTemplate;

    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.service.RateLimitingService rateLimitingService;

    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoads() {
        assertTrue(context.getBeanDefinitionCount() > 0, "an empty context is not a started one");
    }
}
