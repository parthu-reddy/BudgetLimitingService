package com.fooddelivery.advertisement.pacing;

import com.fooddelivery.common.test.AutoConfigurationImports;
import org.junit.jupiter.api.Test;

/**
 * Every auto-configuration this service inherits resolves on its classpath.
 *
 * <p>BudgetLimitingService has no full-context test, so nothing here ever started Spring. That made it
 * one of seven services where the 2026-09-12 CommonLibrary split was verified by reading poms rather
 * than by observing a boot — and an {@code AutoConfiguration.imports} entry naming an absent class
 * fails at startup, not at compile.
 *
 * <p>This closes that specific gap without a context: it reads every {@code .imports} on the
 * classpath and loads each class named. It is not a substitute for a context-load test, and does not
 * pretend to be; it is the part of one that can be written for a service whose context needs
 * infrastructure this build does not have.
 */
class AutoConfigurationImportsTest {

    @Test
    void everyInheritedAutoConfigurationIsOnTheClasspath() {
        AutoConfigurationImports.assertEveryImportResolves();
    }
}
