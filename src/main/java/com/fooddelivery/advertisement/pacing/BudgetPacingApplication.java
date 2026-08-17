package com.fooddelivery.advertisement.pacing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.context.annotation.Import;
import com.fooddelivery.common.service.NotificationRouterService;

import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"com.fooddelivery.advertisement.pacing", "com.fooddelivery"})
@EnableScheduling
@EnableFeignClients
@EntityScan(basePackages = "com.fooddelivery")
@EnableJpaRepositories(basePackages = "com.fooddelivery")
@Import(NotificationRouterService.class)
public class BudgetPacingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BudgetPacingApplication.class, args);
    }
}
