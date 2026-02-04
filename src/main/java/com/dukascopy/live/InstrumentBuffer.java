package com.dukascopy.live;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Holds all timeframe buffers for a single instrument.
 */
public class InstrumentBuffer {
    private final String instrumentName;
    private final Map<TimeframeType, TimeframeBuffer> buffers;
    private final int capacity;

    public InstrumentBuffer(String instrumentName, int[] enabledTimeframes, int capacity) {
        this.instrumentName = instrumentName;
        this.capacity = capacity;
        this.buffers = new EnumMap<>(TimeframeType.class);

        for (int seconds : enabledTimeframes) {
            TimeframeType tf = TimeframeType.fromSeconds(seconds);
            if (tf != null) {
                buffers.put(tf, new TimeframeBuffer(tf, capacity));
            }
        }
    }

    /**
     * Get the buffer for a specific timeframe.
     */
    public TimeframeBuffer getBuffer(TimeframeType timeframe) {
        return buffers.get(timeframe);
    }

    /**
     * Check if a timeframe is enabled.
     */
    public boolean hasTimeframe(TimeframeType timeframe) {
        return buffers.containsKey(timeframe);
    }

    /**
     * Get all enabled timeframes.
     */
    public TimeframeType[] getEnabledTimeframes() {
        return buffers.keySet().toArray(new TimeframeType[0]);
    }

    public String getInstrumentName() {
        return instrumentName;
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * Clear all buffers.
     */
    public void clear() {
        for (TimeframeBuffer buffer : buffers.values()) {
            buffer.clear();
        }
    }

    /**
     * Convert to JSON-serializable map.
     */
    public Map<String, Object> toJsonMap() {
        Map<String, Object> result = new LinkedHashMap<>();

        for (TimeframeType tf : TimeframeType.values()) {
            TimeframeBuffer buffer = buffers.get(tf);
            if (buffer != null) {
                Map<String, Object> tfData = new LinkedHashMap<>();
                tfData.put("mid", buffer.getMidValues());
                tfData.put("ts", formatTimestamps(buffer.getTimestamps()));
                if (tf.hasSpread()) {
                    double[] spreadValues = buffer.getSpreadValues();
                    if (spreadValues != null) {
                        tfData.put("spread", spreadValues);
                    }
                }
                result.put(tf.name(), tfData);
            }
        }

        return result;
    }

    /**
     * Format epoch seconds array to string array in "yyyy.MM.dd HH:mm:ss" EET format.
     */
    private String[] formatTimestamps(long[] epochSeconds) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("EET"));

        String[] result = new String[epochSeconds.length];
        for (int i = 0; i < epochSeconds.length; i++) {
            if (epochSeconds[i] > 0) {
                result[i] = sdf.format(new Date(epochSeconds[i] * 1000));
            } else {
                result[i] = null;
            }
        }
        return result;
    }

    /**
     * Create a deep copy of this instrument buffer.
     */
    public InstrumentBuffer copy() {
        int[] enabledTimeframes = new int[buffers.size()];
        int i = 0;
        for (TimeframeType tf : buffers.keySet()) {
            enabledTimeframes[i++] = tf.getSeconds();
        }

        InstrumentBuffer copy = new InstrumentBuffer(instrumentName, enabledTimeframes, capacity);
        for (Map.Entry<TimeframeType, TimeframeBuffer> entry : buffers.entrySet()) {
            TimeframeBuffer copyBuffer = copy.buffers.get(entry.getKey());
            if (copyBuffer != null) {
                copyBuffer.copyFrom(entry.getValue());
            }
        }
        return copy;
    }

    /**
     * Restore state from another instrument buffer.
     */
    public void copyFrom(InstrumentBuffer other) {
        for (Map.Entry<TimeframeType, TimeframeBuffer> entry : buffers.entrySet()) {
            TimeframeBuffer otherBuffer = other.buffers.get(entry.getKey());
            if (otherBuffer != null) {
                entry.getValue().copyFrom(otherBuffer);
            }
        }
    }

    /**
     * Add S1 data and update all higher timeframes.
     * Higher timeframes are calculated as averages from their parent timeframe
     * when a period boundary is reached.
     */
    public void addS1AndUpdateHigher(long epochSecond, double mid, double spread) {
        // Add to S1 buffer
        TimeframeBuffer s1Buffer = buffers.get(TimeframeType.S1);
        if (s1Buffer != null) {
            s1Buffer.add(epochSecond, mid, spread);
        }

        // Update higher timeframes
        for (TimeframeType tf : TimeframeType.values()) {
            if (tf == TimeframeType.S1) continue;

            TimeframeBuffer tfBuffer = buffers.get(tf);
            if (tfBuffer == null) continue;

            int tfSeconds = tf.getSeconds();
            // Check if we're at the end of a period for this timeframe
            // Period ends when (epochSecond + 1) is divisible by tfSeconds
            if ((epochSecond + 1) % tfSeconds == 0) {
                // Calculate average from S1 buffer over the period
                long periodStart = epochSecond - tfSeconds + 1;
                double avgMid = calculateS1Average(periodStart, epochSecond);
                tfBuffer.add(periodStart, avgMid);
            }
        }
    }

    /**
     * Calculate average of S1 mid values from startEpoch to endEpoch (inclusive).
     * Returns -1 if no valid values found.
     */
    private double calculateS1Average(long startEpoch, long endEpoch) {
        TimeframeBuffer s1Buffer = buffers.get(TimeframeType.S1);
        if (s1Buffer == null) {
            return -1;
        }

        RingBuffer midBuffer = s1Buffer.getMidBuffer();
        double[] values = midBuffer.getValues();
        long[] timestamps = midBuffer.getTimestamps();

        double sum = 0;
        int count = 0;

        for (int i = 0; i < values.length; i++) {
            long ts = timestamps[i];
            if (ts >= startEpoch && ts <= endEpoch && values[i] >= 0) {
                sum += values[i];
                count++;
            }
        }

        return count > 0 ? sum / count : -1;
    }

    /**
     * Check if any buffer contains negative values (disconnect markers).
     */
    public boolean containsNegative() {
        for (TimeframeBuffer buffer : buffers.values()) {
            if (buffer.getMidBuffer().containsNegative()) {
                return true;
            }
        }
        return false;
    }
}
