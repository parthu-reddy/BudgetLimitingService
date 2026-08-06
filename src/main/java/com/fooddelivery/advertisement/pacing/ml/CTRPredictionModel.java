package com.fooddelivery.advertisement.pacing.ml;

import org.springframework.stereotype.Component;

@Component
public class CTRPredictionModel {
    private static final double EPSILON = 0.10; // 10% exploration

    // Mock ML inference for predicting Click-Through-Rate
    public double predictCTR(String campaignId, String context) {
        try {
            // In reality, calls an inference endpoint or uses ONNX model
            // Simulating a potential failure
            if (Math.random() < 0.01) {
                throw new RuntimeException("Model inference failed");
            }
            return Math.random() * 0.05; // 0 to 5% CTR
        } catch (Exception e) {
            // Epsilon-greedy fallback
            if (Math.random() < EPSILON) {
                // Exploration: Assign a slightly optimistic random CTR to gather data
                return 0.01 + (Math.random() * 0.04);
            } else {
                // Exploitation: We don't know the CTR, so we return a conservative default
                return 0.005; // 0.5% default CTR
            }
        }
    }
}
