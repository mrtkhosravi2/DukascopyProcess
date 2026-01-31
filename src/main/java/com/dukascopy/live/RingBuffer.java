package com.dukascopy.live;

public class RingBuffer {
    private static final double MISSING_VALUE = -1.0;

    private final int capacity;
    private final double[] values;
    private final long[] timestamps;
    private int head;
    private int size;

    public RingBuffer(int capacity) {
        this.capacity = capacity;
        this.values = new double[capacity];
        this.timestamps = new long[capacity];
        this.head = 0;
        this.size = 0;

        for (int i = 0; i < capacity; i++) {
            values[i] = MISSING_VALUE;
            timestamps[i] = -1L;
        }
    }

    public void add(long epochSecond, double value) {
        values[head] = value;
        timestamps[head] = epochSecond;
        head = (head + 1) % capacity;
        if (size < capacity) {
            size++;
        }
    }

    public double[] getValues() {
        double[] result = new double[capacity];
        for (int i = 0; i < capacity; i++) {
            int idx = (head - capacity + i + capacity) % capacity;
            result[i] = values[idx];
        }
        return result;
    }

    public long[] getTimestamps() {
        long[] result = new long[capacity];
        for (int i = 0; i < capacity; i++) {
            int idx = (head - capacity + i + capacity) % capacity;
            result[i] = timestamps[idx];
        }
        return result;
    }

    public double getLatest() {
        if (size == 0) {
            return MISSING_VALUE;
        }
        int idx = (head - 1 + capacity) % capacity;
        return values[idx];
    }

    public long getLatestTimestamp() {
        if (size == 0) {
            return -1L;
        }
        int idx = (head - 1 + capacity) % capacity;
        return timestamps[idx];
    }

    public double getValueAt(int index) {
        if (index < 0 || index >= capacity) {
            return MISSING_VALUE;
        }
        int idx = (head - capacity + index + capacity) % capacity;
        return values[idx];
    }

    public long getTimestampAt(int index) {
        if (index < 0 || index >= capacity) {
            return -1L;
        }
        int idx = (head - capacity + index + capacity) % capacity;
        return timestamps[idx];
    }

    public void clear() {
        head = 0;
        size = 0;
        for (int i = 0; i < capacity; i++) {
            values[i] = MISSING_VALUE;
            timestamps[i] = -1L;
        }
    }

    public int getSize() {
        return size;
    }

    public int getCapacity() {
        return capacity;
    }

    public long getOldestTimestamp() {
        if (size == 0) {
            return -1L;
        }
        int idx = (head - size + capacity) % capacity;
        return timestamps[idx];
    }

    public long getNewestTimestamp() {
        return getLatestTimestamp();
    }
}
