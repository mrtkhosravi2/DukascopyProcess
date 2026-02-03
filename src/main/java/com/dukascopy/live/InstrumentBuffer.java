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
}
