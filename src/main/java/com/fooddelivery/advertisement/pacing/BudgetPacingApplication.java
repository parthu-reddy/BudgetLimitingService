package com.fooddelivery.advertisement.pacing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.context.annotation.Import;
import com.fooddelivery.common.service.NotificationRouterService;

import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication(scanBasePackages = {"com.fooddelivery.advertisement.pacing"})
@EnableScheduling
@EnableFeignClients
@Import(NotificationRouterService.class)
public class BudgetPacingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BudgetPacingApplication.class, args);
    }
}
