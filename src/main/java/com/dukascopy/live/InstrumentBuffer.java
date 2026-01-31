package com.dukascopy.live;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class InstrumentBuffer {
    private final String instrumentName;
    private final Map<TimeframeType, TimeframeBuffer> buffers;
    private final int[] enabledTimeframes;
    private final int capacity;

    public InstrumentBuffer(String instrumentName, int[] enabledTimeframes, int capacity) {
        this.instrumentName = instrumentName;
        this.enabledTimeframes = enabledTimeframes;
        this.capacity = capacity;
        this.buffers = new EnumMap<>(TimeframeType.class);

        for (int seconds : enabledTimeframes) {
            TimeframeType tf = TimeframeType.fromSeconds(seconds);
            if (tf != null) {
                buffers.put(tf, new TimeframeBuffer(tf, capacity));
            }
        }
    }

    public void processTick(long epochSecond, double mid, double spread) {
        TimeframeBuffer s1Buffer = buffers.get(TimeframeType.S1);
        if (s1Buffer != null) {
            s1Buffer.addTick(epochSecond, mid, spread);
        }
    }

    public void finalizeSecond(long epochSecond) {
        TimeframeBuffer s1Buffer = buffers.get(TimeframeType.S1);
        if (s1Buffer != null && s1Buffer.hasPendingData()) {
            double avgMid = s1Buffer.finalizePeriod();
            if (avgMid != -1.0) {
                propagateToHigherTimeframes(epochSecond, avgMid);
            }
        }
    }

    private void propagateToHigherTimeframes(long epochSecond, double mid) {
        for (TimeframeType tf : TimeframeType.values()) {
            if (tf == TimeframeType.S1) continue;
            TimeframeBuffer buffer = buffers.get(tf);
            if (buffer != null) {
                buffer.addAggregatedValue(epochSecond, mid);
            }
        }
    }

    public void addHistoricalTick(long epochSecond, double mid, double spread) {
        TimeframeBuffer s1Buffer = buffers.get(TimeframeType.S1);
        if (s1Buffer != null) {
            s1Buffer.addDirectValue(epochSecond, mid, spread);
        }

        for (TimeframeType tf : TimeframeType.values()) {
            if (tf == TimeframeType.S1) continue;
            TimeframeBuffer buffer = buffers.get(tf);
            if (buffer != null) {
                buffer.addAggregatedValue(epochSecond, mid);
            }
        }
    }

    public void finalizeAllPeriods() {
        for (TimeframeBuffer buffer : buffers.values()) {
            if (buffer.hasPendingData()) {
                buffer.finalizePeriod();
            }
        }
    }

    public Map<String, Object> toJsonMap() {
        Map<String, Object> result = new LinkedHashMap<>();

        for (TimeframeType tf : TimeframeType.values()) {
            TimeframeBuffer buffer = buffers.get(tf);
            if (buffer != null) {
                Map<String, double[]> tfData = new LinkedHashMap<>();
                tfData.put("mid", buffer.getMidValues());
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

    public TimeframeBuffer getBuffer(TimeframeType timeframe) {
        return buffers.get(timeframe);
    }

    public String getInstrumentName() {
        return instrumentName;
    }

    public void clear() {
        for (TimeframeBuffer buffer : buffers.values()) {
            buffer.clear();
        }
    }
}
