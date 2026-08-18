package com.fooddelivery.contract;

import com.fooddelivery.advertisement.pacing.client.CampaignClient;
import com.fooddelivery.advertisement.pacing.dto.CampaignPacingDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ActiveProfiles("contract-test")
@SpringBootTest(classes = BudgetLimitingContractConsumerTest.TestConfig.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@AutoConfigureStubRunner(ids = "com.fooddelivery:campaign-service:+:stubs:8095", stubsMode = StubRunnerProperties.StubsMode.LOCAL)
public class BudgetLimitingContractConsumerTest {


    @Configuration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            DataSourceTransactionManagerAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    @EnableFeignClients(clients = CampaignClient.class)
    @org.springframework.context.annotation.Import(com.fooddelivery.advertisement.pacing.client.CampaignClientFallback.class)
    static class TestConfig {
    }

    @Autowired
    private CampaignClient campaignClient;

    @Test
    public void shouldReturnDailyBudgets() {
        Map<String, CampaignPacingDTO> response = campaignClient.getDailyBudgets(Arrays.asList("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"));
        
        assertNotNull(response);
        assertEquals(2, response.size());
        
        CampaignPacingDTO cmp1 = response.get("11111111-1111-1111-1111-111111111111");
        assertNotNull(cmp1);
        assertEquals(500.00, cmp1.getDailyBudget());
        assertEquals("550e8400-e29b-41d4-a716-446655440000", cmp1.getAdvertiserId().toString());

        CampaignPacingDTO cmp2 = response.get("22222222-2222-2222-2222-222222222222");
        assertNotNull(cmp2);
        assertEquals(1500.50, cmp2.getDailyBudget());
        assertEquals("550e8400-e29b-41d4-a716-446655440001", cmp2.getAdvertiserId().toString());
    }
}
