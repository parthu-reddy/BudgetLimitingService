package com.fooddelivery.advertisement.pacing.client;

import com.fooddelivery.common.dto.campaign.CampaignPacingDTO;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;

@Component("budgetlimitingCampaignClientFallback")
public class CampaignClientFallback implements CampaignClient {
    @Override
    public Map<String, CampaignPacingDTO> getDailyBudgets(List<String> campaignIds) {
        throw new IllegalStateException("Campaign service is currently unavailable. Failing fast to ensure financial integrity.");
    }
}
