package contracts.messaging

/*
 * Contract for AD_CAMPAIGN_BUDGET_EXHAUSTED event emitted by BudgetLimitingService.
 * Uses the canonical CampaignChangedEvent schema.
 */
org.springframework.cloud.contract.spec.Contract.make {
    description("Should publish AD_CAMPAIGN_BUDGET_EXHAUSTED to ad-events")
    label("ad_events_exhausted")
    input { triggeredBy('fireBudgetExhaustedEvent()') }
    outputMessage {
        sentTo('ad-events')
        headers {
            header('eventType', 'AD_CAMPAIGN_BUDGET_EXHAUSTED')
            header('aggregateType', 'ADVERTISEMENT')
        }
        body([
            campaignId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            advertiserId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            budgetExhausted: true
        ])
    }
}
