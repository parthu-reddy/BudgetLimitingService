package com.fooddelivery.ad.pacing;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fooddelivery.advertisement.pacing.BudgetPacingApplication;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("contract-test")
@SpringBootTest(classes = BudgetPacingApplication.class)
class BudgetPacingServiceApplicationTests {

    @MockBean
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void contextLoads() {
    }

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void outboxProcessorBeanExists() {
        assertTrue(applicationContext.containsBean("outboxProcessor"), "OutboxProcessor bean should be present");
    }
}
