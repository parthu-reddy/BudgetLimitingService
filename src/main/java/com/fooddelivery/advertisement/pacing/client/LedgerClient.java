package com.fooddelivery.advertisement.pacing.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import java.util.List;
import java.util.Map;

@FeignClient(name = "ledger-service", url = "${ledger.service.url:http://ledger-service:8080}")
public interface LedgerClient {
    
    @GetMapping("/api/v1/ledger/campaign/{campaignId}/spend")
    double getCurrentSpend(@PathVariable("campaignId") String campaignId);

    @GetMapping("/api/v1/ledger/campaign/spend/batch")
    Map<String, Double> getCurrentSpendBatch(@RequestParam("campaignIds") List<String> campaignIds);

    @GetMapping("/api/v1/ledger/campaign/budget/daily/batch")
    Map<String, Double> getDailyBudgets(@RequestParam("campaignIds") List<String> campaignIds);
    
}
