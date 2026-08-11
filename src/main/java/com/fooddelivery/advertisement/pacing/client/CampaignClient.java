package com.fooddelivery.advertisement.pacing.client;

import com.fooddelivery.advertisement.pacing.dto.CampaignPacingDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import java.util.List;
import java.util.Map;

@FeignClient(name = "campaign-service", url = "${campaign-service.url:http://campaign-service}", fallback = CampaignClientFallback.class)
public interface CampaignClient {

    @PostMapping("/api/v1/internal/campaigns/batch/budgets")
    Map<String, CampaignPacingDTO> getDailyBudgets(@RequestBody List<String> campaignIds);
    
}
