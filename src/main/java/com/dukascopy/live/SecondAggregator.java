package com.dukascopy.live;

/**
 * Accumulates ticks within a single second for one instrument.
 * Thread-safe for concurrent tick additions.
 */
public class SecondAggregator {
    private double midSum = 0;
    private double spreadSum = 0;
    private int tickCount = 0;
    private final Object lock = new Object();

    /**
     * Add a tick to the aggregator.
     */
    public void addTick(double mid, double spread) {
        synchronized (lock) {
            midSum += mid;
            spreadSum += spread;
            tickCount++;
        }
    }

    /**
     * Finalize the current second and reset for the next.
     * Returns the aggregated result, or null if no ticks were received.
     */
    public AggregatedSecond finalizeAndReset() {
        synchronized (lock) {
            if (tickCount == 0) {
                return null;
            }

            double avgMid = midSum / tickCount;
            double avgSpread = spreadSum / tickCount;

            // Reset for next second
            midSum = 0;
            spreadSum = 0;
            tickCount = 0;

            return new AggregatedSecond(avgMid, avgSpread);
        }
    }

    /**
     * Check if any ticks have been received this second.
     */
    public boolean hasTicks() {
        synchronized (lock) {
            return tickCount > 0;
        }
    }

    /**
     * Result of aggregating ticks for one second.
     */
    public static class AggregatedSecond {
        public final double mid;
        public final double spread;

        public AggregatedSecond(double mid, double spread) {
            this.mid = mid;
            this.spread = spread;
        }
    }
}
