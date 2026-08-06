package com.fooddelivery.advertisement.pacing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableScheduling
@EnableFeignClients
public class BudgetPacingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BudgetPacingApplication.class, args);
    }
}
