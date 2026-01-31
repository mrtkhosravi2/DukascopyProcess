package com.dukascopy.live;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimeframeBuffer {
    private static final double MISSING_VALUE = -1.0;

    private final TimeframeType timeframe;
    private final RingBuffer midBuffer;
    private final RingBuffer spreadBuffer;
    private final ReentrantReadWriteLock lock;

    // Period accumulator for aggregation
    private double midSum;
    private double spreadSum;
    private int tickCount;
    private long currentPeriodStart;

    public TimeframeBuffer(TimeframeType timeframe, int capacity) {
        this.timeframe = timeframe;
        this.midBuffer = new RingBuffer(capacity);
        this.spreadBuffer = timeframe.hasSpread() ? new RingBuffer(capacity) : null;
        this.lock = new ReentrantReadWriteLock();

        resetAccumulator();
    }

    private void resetAccumulator() {
        midSum = 0.0;
        spreadSum = 0.0;
        tickCount = 0;
        currentPeriodStart = -1L;
    }

    private long getPeriodStart(long epochSecond) {
        int periodSeconds = timeframe.getSeconds();
        return (epochSecond / periodSeconds) * periodSeconds;
    }

    public void addTick(long epochSecond, double mid, double spread) {
        lock.writeLock().lock();
        try {
            long periodStart = getPeriodStart(epochSecond);

            if (currentPeriodStart == -1L) {
                currentPeriodStart = periodStart;
            }

            if (periodStart != currentPeriodStart) {
                finalizePeriod();
                currentPeriodStart = periodStart;
            }

            midSum += mid;
            if (spreadBuffer != null) {
                spreadSum += spread;
            }
            tickCount++;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addAggregatedValue(long epochSecond, double mid) {
        lock.writeLock().lock();
        try {
            long periodStart = getPeriodStart(epochSecond);

            if (currentPeriodStart == -1L) {
                currentPeriodStart = periodStart;
            }

            if (periodStart != currentPeriodStart) {
                finalizePeriod();
                currentPeriodStart = periodStart;
            }

            midSum += mid;
            tickCount++;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addMissingValue(long epochSecond) {
        lock.writeLock().lock();
        try {
            midBuffer.add(epochSecond, MISSING_VALUE);
            if (spreadBuffer != null) {
                spreadBuffer.add(epochSecond, MISSING_VALUE);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addDirectValue(long epochSecond, double mid, double spread) {
        lock.writeLock().lock();
        try {
            midBuffer.add(epochSecond, mid);
            if (spreadBuffer != null) {
                spreadBuffer.add(epochSecond, spread);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addDirectValue(long epochSecond, double mid) {
        lock.writeLock().lock();
        try {
            midBuffer.add(epochSecond, mid);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public double finalizePeriod() {
        lock.writeLock().lock();
        try {
            if (tickCount > 0) {
                double avgMid = midSum / tickCount;
                midBuffer.add(currentPeriodStart, avgMid);

                if (spreadBuffer != null) {
                    double avgSpread = spreadSum / tickCount;
                    spreadBuffer.add(currentPeriodStart, avgSpread);
                }

                double result = avgMid;
                resetAccumulator();
                return result;
            }
            return MISSING_VALUE;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean hasPendingData() {
        lock.readLock().lock();
        try {
            return tickCount > 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getCurrentPeriodStart() {
        lock.readLock().lock();
        try {
            return currentPeriodStart;
        } finally {
            lock.readLock().unlock();
        }
    }

    public double[] getMidValues() {
        lock.readLock().lock();
        try {
            return midBuffer.getValues();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long[] getTimestamps() {
        lock.readLock().lock();
        try {
            return midBuffer.getTimestamps();
        } finally {
            lock.readLock().unlock();
        }
    }

    public double[] getSpreadValues() {
        lock.readLock().lock();
        try {
            if (spreadBuffer != null) {
                return spreadBuffer.getValues();
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getOldestTimestamp() {
        lock.readLock().lock();
        try {
            return midBuffer.getOldestTimestamp();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getNewestTimestamp() {
        lock.readLock().lock();
        try {
            return midBuffer.getNewestTimestamp();
        } finally {
            lock.readLock().unlock();
        }
    }

    public double getLatestMid() {
        lock.readLock().lock();
        try {
            return midBuffer.getLatest();
        } finally {
            lock.readLock().unlock();
        }
    }

    public TimeframeType getTimeframe() {
        return timeframe;
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            midBuffer.clear();
            if (spreadBuffer != null) {
                spreadBuffer.clear();
            }
            resetAccumulator();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
