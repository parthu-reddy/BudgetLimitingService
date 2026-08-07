package com.fooddelivery.advertisement.pacing.dto;

import java.util.UUID;

public class CampaignPacingDTO {
    private Double dailyBudget;
    private UUID advertiserId;

    public CampaignPacingDTO() {}

    public CampaignPacingDTO(Double dailyBudget, UUID advertiserId) {
        this.dailyBudget = dailyBudget;
        this.advertiserId = advertiserId;
    }

    public Double getDailyBudget() {
        return dailyBudget;
    }

    public void setDailyBudget(Double dailyBudget) {
        this.dailyBudget = dailyBudget;
    }

    public UUID getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(UUID advertiserId) {
        this.advertiserId = advertiserId;
    }
}
