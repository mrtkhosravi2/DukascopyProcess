package com.dukascopy.live;

/**
 * A fixed-size ring buffer for storing timestamped values.
 * New values are added to the end, and when full, the oldest values are overwritten.
 */
public class RingBuffer {
    private final int capacity;
    private final double[] values;
    private final long[] timestamps;
    private int head;  // Points to where next value will be written
    private int size;

    public RingBuffer(int capacity) {
        this.capacity = capacity;
        this.values = new double[capacity];
        this.timestamps = new long[capacity];
        this.head = 0;
        this.size = 0;
    }

    /**
     * Add a value with its timestamp to the buffer.
     */
    public void add(long epochSecond, double value) {
        values[head] = value;
        timestamps[head] = epochSecond;
        head = (head + 1) % capacity;
        if (size < capacity) {
            size++;
        }
    }

    /**
     * Get all values in chronological order (oldest to newest).
     * Returns array of size 'size', not capacity.
     */
    public double[] getValues() {
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            int idx = (head - size + i + capacity) % capacity;
            result[i] = values[idx];
        }
        return result;
    }

    /**
     * Get all timestamps in chronological order (oldest to newest).
     * Returns array of size 'size', not capacity.
     */
    public long[] getTimestamps() {
        long[] result = new long[size];
        for (int i = 0; i < size; i++) {
            int idx = (head - size + i + capacity) % capacity;
            result[i] = timestamps[idx];
        }
        return result;
    }

    /**
     * Get the most recently added value.
     */
    public double getLatest() {
        if (size == 0) {
            return Double.NaN;
        }
        int idx = (head - 1 + capacity) % capacity;
        return values[idx];
    }

    /**
     * Get the most recently added timestamp.
     */
    public long getLatestTimestamp() {
        if (size == 0) {
            return -1L;
        }
        int idx = (head - 1 + capacity) % capacity;
        return timestamps[idx];
    }

    /**
     * Get the oldest value in the buffer.
     */
    public double getOldest() {
        if (size == 0) {
            return Double.NaN;
        }
        int idx = (head - size + capacity) % capacity;
        return values[idx];
    }

    /**
     * Get the oldest timestamp in the buffer.
     */
    public long getOldestTimestamp() {
        if (size == 0) {
            return -1L;
        }
        int idx = (head - size + capacity) % capacity;
        return timestamps[idx];
    }

    /**
     * Get value at a specific index (0 = oldest, size-1 = newest).
     */
    public double getValueAt(int index) {
        if (index < 0 || index >= size) {
            return Double.NaN;
        }
        int idx = (head - size + index + capacity) % capacity;
        return values[idx];
    }

    /**
     * Get timestamp at a specific index (0 = oldest, size-1 = newest).
     */
    public long getTimestampAt(int index) {
        if (index < 0 || index >= size) {
            return -1L;
        }
        int idx = (head - size + index + capacity) % capacity;
        return timestamps[idx];
    }

    public void clear() {
        head = 0;
        size = 0;
    }

    public int getSize() {
        return size;
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean isFull() {
        return size == capacity;
    }
}
