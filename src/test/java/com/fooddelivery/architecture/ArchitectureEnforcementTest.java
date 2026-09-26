package com.fooddelivery.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;
import com.tngtech.archunit.lang.ArchRule;
import com.fooddelivery.common.architecture.ArchUnitRules;
import com.fooddelivery.common.architecture.TimeDisciplineRules;

@AnalyzeClasses(
    packages = "com.fooddelivery",
    importOptions = {
        ImportOption.DoNotIncludeTests.class,
        ImportOption.DoNotIncludeJars.class,
        ImportOption.DoNotIncludeArchives.class
    }
)
public class ArchitectureEnforcementTest {

    @ArchTest
    public static final ArchRule layered_architecture_is_respected = 
        ArchUnitRules.getBaseLayeredArchitecture();

    // Instants for moments, no ambient zone, crons that name their zone; plus a check that this test
    // JVM runs in the hostile zone. RandomDocuments/TimezoneCorrectness_2026-09-25.
    @ArchTest
    static final ArchTests time_discipline = ArchTests.in(TimeDisciplineRules.class);

}
