package com.dukascopy.live;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores tick data and S1 aggregated data for a single hour.
 */
public class HourData {
    private final long hourStartEpoch;  // Start of hour in epoch seconds (EET)
    private final List<Tick> ticks;
    private boolean valid;

    // S1 data: 3600 entries (one per second)
    private double[] s1Mid;
    private double[] s1Spread;

    public static class Tick {
        public final long epochSecond;
        public final double mid;
        public final double spread;

        public Tick(long epochSecond, double mid, double spread) {
            this.epochSecond = epochSecond;
            this.mid = mid;
            this.spread = spread;
        }
    }

    public HourData(long hourStartEpoch) {
        this.hourStartEpoch = hourStartEpoch;
        this.ticks = new ArrayList<>();
        this.valid = false;
    }

    public void addTick(long epochSecond, double mid, double spread) {
        ticks.add(new Tick(epochSecond, mid, spread));
    }

    public boolean hasTicks() {
        return !ticks.isEmpty();
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public boolean isValid() {
        return valid;
    }

    public long getHourStartEpoch() {
        return hourStartEpoch;
    }

    public List<Tick> getTicks() {
        return ticks;
    }

    /**
     * Generate S1 data from raw ticks.
     * Each second will have exactly one value:
     * - Average of all ticks in that second if ticks exist
     * - Forward-filled from last valid second if no ticks
     *
     * @param lastValidMid Last valid mid value for forward-fill (can be NaN if none)
     * @param lastValidSpread Last valid spread value for forward-fill (can be NaN if none)
     * @return Array of [lastMid, lastSpread] after processing this hour
     */
    public double[] generateS1Data(double lastValidMid, double lastValidSpread) {
        s1Mid = new double[3600];
        s1Spread = new double[3600];

        // Group ticks by second offset within hour
        double[] midSum = new double[3600];
        double[] spreadSum = new double[3600];
        int[] count = new int[3600];

        for (Tick tick : ticks) {
            int secondOffset = (int) (tick.epochSecond - hourStartEpoch);
            if (secondOffset >= 0 && secondOffset < 3600) {
                midSum[secondOffset] += tick.mid;
                spreadSum[secondOffset] += tick.spread;
                count[secondOffset]++;
            }
        }

        // Generate S1 values with forward-fill
        double currentMid = lastValidMid;
        double currentSpread = lastValidSpread;

        for (int i = 0; i < 3600; i++) {
            if (count[i] > 0) {
                // Average ticks for this second
                currentMid = midSum[i] / count[i];
                currentSpread = spreadSum[i] / count[i];
            }
            // If no ticks and no valid previous value, values remain NaN
            s1Mid[i] = currentMid;
            s1Spread[i] = currentSpread;
        }

        return new double[] { currentMid, currentSpread };
    }

    /**
     * Get S1 mid values (3600 entries, one per second).
     * Must call generateS1Data first.
     */
    public double[] getS1Mid() {
        return s1Mid;
    }

    /**
     * Get S1 spread values (3600 entries, one per second).
     * Must call generateS1Data first.
     */
    public double[] getS1Spread() {
        return s1Spread;
    }

    /**
     * Get S1 mid value at specific second offset (0-3599).
     */
    public double getS1MidAt(int secondOffset) {
        if (s1Mid == null || secondOffset < 0 || secondOffset >= 3600) {
            return Double.NaN;
        }
        return s1Mid[secondOffset];
    }

    /**
     * Get S1 spread value at specific second offset (0-3599).
     */
    public double getS1SpreadAt(int secondOffset) {
        if (s1Spread == null || secondOffset < 0 || secondOffset >= 3600) {
            return Double.NaN;
        }
        return s1Spread[secondOffset];
    }

    /**
     * Clear S1 data to free memory (for invalid hours).
     */
    public void clearS1Data() {
        s1Mid = null;
        s1Spread = null;
    }

    /**
     * Get the epoch second for a given second offset within this hour.
     */
    public long getEpochSecondAt(int secondOffset) {
        return hourStartEpoch + secondOffset;
    }
}
