package com.fooddelivery.advertisement.pacing.dto;

import java.util.UUID;

public class CampaignPacingDTO {
    private Double dailyBudget;
    private Double lifetimeBudget;
    private UUID advertiserId;

    public CampaignPacingDTO() {}

    public CampaignPacingDTO(Double dailyBudget, Double lifetimeBudget, UUID advertiserId) {
        this.dailyBudget = dailyBudget;
        this.lifetimeBudget = lifetimeBudget;
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

    public Double getLifetimeBudget() {
        return lifetimeBudget;
    }

    public void setLifetimeBudget(Double lifetimeBudget) {
        this.lifetimeBudget = lifetimeBudget;
    }
}
