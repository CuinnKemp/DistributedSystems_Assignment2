package com.distributedsystems.shared;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for LamportClock
 */
public class LamportClockTest {

    private LamportClock clock;

    @Before
    public void setUp() {
        clock = new LamportClock();
    }

    @Test
    public void testInitialValueIsZero() {
        assertEquals(0, clock.get());
    }

    @Test
    public void testTickIncrementsTime() {
        clock.tick();
        assertEquals(1, clock.get());
        clock.tick();
        assertEquals(2, clock.get());
    }

    @Test
    public void testUpdateWithSmallerTimeIncrements() {
        clock.tick(); // 1
        clock.update(0); // max(1,0)+1 = 2
        assertEquals(2, clock.get());
    }

    @Test
    public void testUpdateWithLargerTime() {
        clock.update(5); // max(0,5)+1 = 6
        assertEquals(6, clock.get());
    }

    @Test
    public void testSequentialUpdates() {
        clock.update(3); // 4
        assertEquals(4, clock.get());

        clock.update(10); // 11
        assertEquals(11, clock.get());

        clock.tick(); // 12
        assertEquals(12, clock.get());
    }

    @Test
    public void testConcurrentSafety() throws InterruptedException {
        // Run ticks in parallel to test synchronization
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                clock.tick();
            }
        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                clock.tick();
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertEquals(2000, clock.get());
    }
}
