package com.github.lechandonga.iotdevicetwingatewayjava.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.BooleanSupplier;

public final class TestFixtures {

    private static final Logger log = LoggerFactory.getLogger("ASSERT");

    private TestFixtures() {
    }

    public static void assertTrue(boolean condition, String scenario, String detail) {
        if (!condition) {
            log.error("[失败] {} — {}", scenario, detail);
            throw new AssertionError(scenario + ": " + detail);
        }
        log.info("[通过] {} — {}", scenario, detail);
    }

    public static void assertEquals(Object expected, Object actual, String scenario) {
        assertTrue(expected == null ? actual == null : expected.equals(actual), scenario,
                "期望=" + expected + " 实际=" + actual);
    }

    public static void waitFor(String description, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("等待超时: " + description);
        }
    }
}
