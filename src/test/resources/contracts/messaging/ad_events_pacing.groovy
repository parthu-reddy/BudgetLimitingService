package contracts.messaging

/*
 * Contract for AD_CAMPAIGN_PACING_UPDATED event emitted by BudgetLimitingService.
 * Uses the canonical CampaignChangedEvent schema.
 */
org.springframework.cloud.contract.spec.Contract.make {
    description("Should publish AD_CAMPAIGN_PACING_UPDATED to ad-events")
    label("ad_events_pacing")
    input { triggeredBy('firePacingUpdatedEvent()') }
    outputMessage {
        sentTo('ad-events')
        headers {
            header('eventType', 'AD_CAMPAIGN_PACING_UPDATED')
            header('aggregateType', 'ADVERTISEMENT')
        }
        body([
            campaignId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            advertiserId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            pacingMultiplier: 0.85,
            budgetExhausted: false
        ])
    }
}
