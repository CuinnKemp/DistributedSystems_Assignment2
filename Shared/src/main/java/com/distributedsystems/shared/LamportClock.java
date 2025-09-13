package com.distributedsystems.shared;

/**
 * Simple Lamport time object
 */
public class LamportClock {
    private int time = 0;

    /**
     * increase clock by 1 time stamp
     */
    public synchronized void tick() {
        time++;
    }

    /**
     * update lamport time using received time
     *
     * @param received lamport time in request
     */
    public synchronized void update(int received) {
        time = Math.max(time, received) + 1;
    }

    /**
     * return the current lamport time
     *
     * @return the lamport time
     */
    public synchronized int get() {
        return time;
    }
}

