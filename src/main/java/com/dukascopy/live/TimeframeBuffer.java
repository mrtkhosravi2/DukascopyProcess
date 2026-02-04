package com.dukascopy.live;

/**
 * Buffer for a specific timeframe, containing a ring buffer for mid values
 * and optionally spread values (only for S1).
 */
public class TimeframeBuffer {
    private final TimeframeType timeframe;
    private final RingBuffer midBuffer;
    private final RingBuffer spreadBuffer;  // Only for S1
    private final int capacity;

    public TimeframeBuffer(TimeframeType timeframe, int capacity) {
        this.timeframe = timeframe;
        this.capacity = capacity;
        this.midBuffer = new RingBuffer(capacity);
        this.spreadBuffer = timeframe.hasSpread() ? new RingBuffer(capacity) : null;
    }

    /**
     * Add a value to the buffer (for S1, includes spread).
     */
    public void add(long epochSecond, double mid, double spread) {
        midBuffer.add(epochSecond, mid);
        if (spreadBuffer != null) {
            spreadBuffer.add(epochSecond, spread);
        }
    }

    /**
     * Add a value to the buffer (for higher timeframes, no spread).
     */
    public void add(long epochSecond, double mid) {
        midBuffer.add(epochSecond, mid);
    }

    /**
     * Get all mid values in chronological order.
     */
    public double[] getMidValues() {
        return midBuffer.getValues();
    }

    /**
     * Get all timestamps in chronological order.
     */
    public long[] getTimestamps() {
        return midBuffer.getTimestamps();
    }

    /**
     * Get all spread values in chronological order (S1 only).
     */
    public double[] getSpreadValues() {
        return spreadBuffer != null ? spreadBuffer.getValues() : null;
    }

    /**
     * Get the most recently added mid value.
     */
    public double getLatestMid() {
        return midBuffer.getLatest();
    }

    /**
     * Get the most recently added timestamp.
     */
    public long getLatestTimestamp() {
        return midBuffer.getLatestTimestamp();
    }

    /**
     * Get the most recently added spread value (S1 only).
     */
    public double getLatestSpread() {
        return spreadBuffer != null ? spreadBuffer.getLatest() : Double.NaN;
    }

    public TimeframeType getTimeframe() {
        return timeframe;
    }

    public int getSize() {
        return midBuffer.getSize();
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean isFull() {
        return midBuffer.isFull();
    }

    public void clear() {
        midBuffer.clear();
        if (spreadBuffer != null) {
            spreadBuffer.clear();
        }
    }

    /**
     * Create a deep copy of this timeframe buffer.
     */
    public TimeframeBuffer copy() {
        TimeframeBuffer copy = new TimeframeBuffer(timeframe, capacity);
        copy.midBuffer.copyFrom(this.midBuffer);
        if (this.spreadBuffer != null && copy.spreadBuffer != null) {
            copy.spreadBuffer.copyFrom(this.spreadBuffer);
        }
        return copy;
    }

    /**
     * Restore state from another timeframe buffer.
     */
    public void copyFrom(TimeframeBuffer other) {
        if (other.timeframe != this.timeframe) {
            throw new IllegalArgumentException("Cannot copy from buffer with different timeframe");
        }
        this.midBuffer.copyFrom(other.midBuffer);
        if (this.spreadBuffer != null && other.spreadBuffer != null) {
            this.spreadBuffer.copyFrom(other.spreadBuffer);
        }
    }

    /**
     * Get the mid buffer (for internal access).
     */
    RingBuffer getMidBuffer() {
        return midBuffer;
    }

    /**
     * Get the spread buffer (for internal access, S1 only).
     */
    RingBuffer getSpreadBuffer() {
        return spreadBuffer;
    }
}
