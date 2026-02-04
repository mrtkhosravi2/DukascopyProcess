package com.dukascopy.live;

import com.dukascopy.api.IHistory;
import com.dukascopy.api.ITick;
import com.dukascopy.api.Instrument;
import com.dukascopy.Config;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Manages the warmup process for live data.
 *
 * Warmup process:
 * 1. Fetch ticks hour-by-hour until required_valid_hours are accumulated
 * 2. Generate S1 data (3600 datapoints per valid hour, forward-filled)
 * 3. Calculate higher timeframes from parent timeframes
 * 4. Populate ring buffers with last lookback_window datapoints
 */
public class WarmupManager {
    private static final TimeZone EET = TimeZone.getTimeZone("EET");

    private final Config config;
    private final IHistory history;
    private final Map<Instrument, InstrumentBuffer> instrumentBuffers;
    private final int requiredValidHours;
    private final int lookbackWindow;

    // Cutoff time - don't include data past this point
    private long maxEpochSecond;

    public WarmupManager(Config config, IHistory history, List<Instrument> instruments) {
        this.config = config;
        this.history = history;
        this.instrumentBuffers = new LinkedHashMap<>();
        this.lookbackWindow = config.getLookbackWindow();

        // Calculate required valid hours: lookback_window // (3600 // largest_timeframe_secs) + 1
        int largestTimeframe = getLargestTimeframe(config.getLiveTimeframes());
        this.requiredValidHours = lookbackWindow / (3600 / largestTimeframe) + 1;

        System.out.println("[" + new Date() + "] WarmupManager: lookbackWindow=" + lookbackWindow +
                ", largestTimeframe=" + largestTimeframe + "s, requiredValidHours=" + requiredValidHours);

        for (Instrument instrument : instruments) {
            instrumentBuffers.put(instrument, new InstrumentBuffer(
                    instrument.toString(),
                    config.getLiveTimeframes(),
                    lookbackWindow
            ));
        }
    }

    private int getLargestTimeframe(int[] timeframes) {
        int max = 1;
        for (int tf : timeframes) {
            if (tf > max) {
                max = tf;
            }
        }
        return max;
    }

    /**
     * Perform warmup for all instruments.
     */
    public void performWarmup() throws Exception {
        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            Instrument instrument = entry.getKey();
            InstrumentBuffer buffer = entry.getValue();

            System.out.println("[" + new Date() + "] Starting warmup for " + instrument);
            performWarmupForInstrument(instrument, buffer);
            System.out.println("[" + new Date() + "] Completed warmup for " + instrument);
        }
    }

    private void performWarmupForInstrument(Instrument instrument, InstrumentBuffer buffer) throws Exception {
        // Step 1: Fetch ticks hour-by-hour until we have enough valid hours
        List<HourData> hours = fetchHoursUntilValid(instrument);

        if (hours.isEmpty()) {
            System.out.println("[" + new Date() + "] Warning: No valid hours found for " + instrument);
            return;
        }

        // Step 2: Generate S1 data for all hours, then remove invalid hours' data
        generateS1Data(hours);

        // Step 3: Calculate higher timeframes from S1
        // Step 4: Populate ring buffers with last lookbackWindow datapoints
        populateBuffers(buffer, hours);
    }

    /**
     * Fetch hours going backwards from current time until we have requiredValidHours valid hours.
     */
    private List<HourData> fetchHoursUntilValid(Instrument instrument) throws Exception {
        List<HourData> hours = new ArrayList<>();
        int validHourCount = 0;

        // Get current time in EET
        long nowMs = System.currentTimeMillis() - 1000;  // 1 second in the past to avoid edge issues
        maxEpochSecond = nowMs / 1000;  // Set cutoff time for buffer population

        Calendar cal = Calendar.getInstance(EET);
        cal.setTimeInMillis(nowMs);

        // Get current hour start
        int currentHourOfDay = cal.get(Calendar.HOUR_OF_DAY);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long currentHourStart = cal.getTimeInMillis();
        long currentHourStartEpoch = currentHourStart / 1000;

        // Track if we're on the first (current) hour
        boolean isCurrentHour = true;
        long hourStart = currentHourStart;
        long hourStartEpoch = currentHourStartEpoch;

        // Track if hour immediately before current hour has ticks
        // (needed for current hour validity rule)
        Boolean previousHourHasTicks = null;
        HourData currentHourData = null;

        while (validHourCount < requiredValidHours) {
            long fetchStart;
            long fetchEnd;

            if (isCurrentHour) {
                // For current hour: fetch from hour start to now-1
                fetchStart = hourStart;
                fetchEnd = nowMs;
            } else {
                // For other hours: fetch full hour
                fetchStart = hourStart;
                fetchEnd = hourStart + 3600 * 1000 - 1;
            }

            // Fetch ticks for this hour
            List<ITick> ticks = history.getTicks(instrument, fetchStart, fetchEnd);
            HourData hourData = new HourData(hourStartEpoch);

            if (ticks != null && !ticks.isEmpty()) {
                for (ITick tick : ticks) {
                    double bid = tick.getBid();
                    double ask = tick.getAsk();
                    if (!Double.isNaN(bid) && !Double.isNaN(ask)) {
                        double mid = (bid + ask) / 2.0;
                        double spread = ask - bid;
                        hourData.addTick(tick.getTime() / 1000, mid, spread);
                    }
                }
            }

            boolean hasTicks = hourData.hasTicks();

            if (isCurrentHour) {
                // Store current hour data for later validity determination
                currentHourData = hourData;
                isCurrentHour = false;
            } else {
                // For non-current hours: valid if has ticks
                if (hasTicks) {
                    hourData.setValid(true);
                    validHourCount++;
                    hours.add(hourData);

                    // Check if this is the hour immediately before current hour
                    if (previousHourHasTicks == null) {
                        previousHourHasTicks = true;
                    }
                } else {
                    // Track first hour after current
                    if (previousHourHasTicks == null) {
                        previousHourHasTicks = false;
                    }
                }
            }

            // Move to previous hour
            hourStart -= 3600 * 1000;
            hourStartEpoch -= 3600;

            // Safety check: don't go back more than 30 days
            if (hours.size() > 24 * 30) {
                System.out.println("[" + new Date() + "] Warning: Reached 30-day limit during warmup");
                break;
            }
        }

        // Now determine current hour validity
        // Current hour is valid if: (a) has ticks, OR (b) immediately previous hour has ticks
        if (currentHourData != null) {
            boolean currentHourValid = currentHourData.hasTicks() ||
                    (previousHourHasTicks != null && previousHourHasTicks);

            if (currentHourValid) {
                currentHourData.setValid(true);
                hours.add(0, currentHourData);  // Add at beginning (most recent)
            }
        }

        // Sort hours by timestamp (oldest first for processing)
        hours.sort((a, b) -> Long.compare(a.getHourStartEpoch(), b.getHourStartEpoch()));

        System.out.println("[" + new Date() + "] Fetched " + hours.size() + " hours, " +
                countValidHours(hours) + " valid");

        return hours;
    }

    private int countValidHours(List<HourData> hours) {
        int count = 0;
        for (HourData hour : hours) {
            if (hour.isValid()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Generate S1 data for all hours, forward-filling gaps.
     * Then clear S1 data for invalid hours.
     */
    private void generateS1Data(List<HourData> hours) {
        double lastMid = Double.NaN;
        double lastSpread = Double.NaN;

        // First pass: generate S1 for all hours
        for (HourData hour : hours) {
            double[] lastValues = hour.generateS1Data(lastMid, lastSpread);
            lastMid = lastValues[0];
            lastSpread = lastValues[1];
        }

        // Second pass: clear S1 data for invalid hours
        for (HourData hour : hours) {
            if (!hour.isValid()) {
                hour.clearS1Data();
            }
        }
    }

    /**
     * Populate the instrument buffer with calculated timeframe data.
     */
    private void populateBuffers(InstrumentBuffer buffer, List<HourData> hours) {
        // Filter to only valid hours
        List<HourData> validHours = new ArrayList<>();
        for (HourData hour : hours) {
            if (hour.isValid()) {
                validHours.add(hour);
            }
        }

        if (validHours.isEmpty()) {
            return;
        }

        // For each timeframe, calculate and populate
        for (TimeframeType tf : buffer.getEnabledTimeframes()) {
            populateTimeframe(buffer, validHours, tf);
        }
    }

    /**
     * Calculate and populate a specific timeframe buffer.
     */
    private void populateTimeframe(InstrumentBuffer buffer, List<HourData> validHours, TimeframeType tf) {
        TimeframeBuffer tfBuffer = buffer.getBuffer(tf);
        if (tfBuffer == null) {
            return;
        }

        if (tf == TimeframeType.S1) {
            // For S1: directly use the S1 data from valid hours
            populateS1Buffer(tfBuffer, validHours);
        } else {
            // For higher timeframes: calculate from parent
            populateHigherTimeframe(buffer, validHours, tf);
        }
    }

    /**
     * Populate S1 buffer with last lookbackWindow values.
     * Only includes data up to maxEpochSecond (excludes future timestamps).
     */
    private void populateS1Buffer(TimeframeBuffer tfBuffer, List<HourData> validHours) {
        // Collect all S1 data points up to maxEpochSecond
        List<double[]> allData = new ArrayList<>();  // [epochSecond, mid, spread]

        for (HourData hour : validHours) {
            double[] s1Mid = hour.getS1Mid();
            double[] s1Spread = hour.getS1Spread();
            if (s1Mid == null) continue;

            for (int i = 0; i < 3600; i++) {
                long epochSecond = hour.getEpochSecondAt(i);
                // Only include data up to the cutoff time
                if (epochSecond > maxEpochSecond) {
                    break;  // This hour's remaining seconds are in the future
                }
                allData.add(new double[] { epochSecond, s1Mid[i], s1Spread[i] });
            }
        }

        // Take last lookbackWindow entries
        int startIdx = Math.max(0, allData.size() - lookbackWindow);
        for (int i = startIdx; i < allData.size(); i++) {
            double[] entry = allData.get(i);
            tfBuffer.add((long) entry[0], entry[1], entry[2]);
        }
    }

    /**
     * Populate a higher timeframe buffer by calculating from parent.
     */
    private void populateHigherTimeframe(InstrumentBuffer buffer, List<HourData> validHours, TimeframeType tf) {
        TimeframeBuffer tfBuffer = buffer.getBuffer(tf);
        if (tfBuffer == null) return;

        TimeframeType parentTf = tf.getParent();
        if (parentTf == null) return;

        int tfSeconds = tf.getSeconds();
        int parentSeconds = parentTf.getSeconds();
        int ratio = tfSeconds / parentSeconds;

        // First, get all parent timeframe values
        List<double[]> parentData = getTimeframeData(buffer, validHours, parentTf);

        if (parentData.isEmpty()) {
            return;
        }

        // Calculate this timeframe from parent
        List<double[]> tfData = new ArrayList<>();

        int i = 0;
        while (i < parentData.size()) {
            long parentTs = (long) parentData.get(i)[0];

            // Find the period start for this timeframe (aligned to hour)
            long periodStart = (parentTs / tfSeconds) * tfSeconds;

            // Collect 'ratio' parent values that belong to this period
            double sum = 0;
            int count = 0;

            while (i < parentData.size() && count < ratio) {
                long ts = (long) parentData.get(i)[0];
                long expectedPeriodStart = (ts / tfSeconds) * tfSeconds;

                if (expectedPeriodStart == periodStart) {
                    sum += parentData.get(i)[1];
                    count++;
                    i++;
                } else {
                    break;
                }
            }

            if (count == ratio) {
                // We have a complete period
                double avg = sum / ratio;
                tfData.add(new double[] { periodStart, avg });
            } else if (count > 0) {
                // Incomplete period - skip to next aligned period
                // Move i back to reprocess
                i -= count;
                // Find next aligned period
                while (i < parentData.size()) {
                    long ts = (long) parentData.get(i)[0];
                    long nextPeriodStart = (ts / tfSeconds) * tfSeconds;
                    if (nextPeriodStart > periodStart) {
                        break;
                    }
                    i++;
                }
            }
        }

        // Take last lookbackWindow entries and add to buffer
        int startIdx = Math.max(0, tfData.size() - lookbackWindow);
        for (int j = startIdx; j < tfData.size(); j++) {
            double[] entry = tfData.get(j);
            tfBuffer.add((long) entry[0], entry[1]);
        }
    }

    /**
     * Get all data points for a timeframe (recursively calculates if needed).
     * Only includes data up to maxEpochSecond.
     */
    private List<double[]> getTimeframeData(InstrumentBuffer buffer, List<HourData> validHours, TimeframeType tf) {
        List<double[]> result = new ArrayList<>();

        if (tf == TimeframeType.S1) {
            // Return all S1 data up to maxEpochSecond
            for (HourData hour : validHours) {
                double[] s1Mid = hour.getS1Mid();
                if (s1Mid == null) continue;

                for (int i = 0; i < 3600; i++) {
                    long epochSecond = hour.getEpochSecondAt(i);
                    // Only include data up to the cutoff time
                    if (epochSecond > maxEpochSecond) {
                        break;
                    }
                    result.add(new double[] { epochSecond, s1Mid[i] });
                }
            }
            return result;
        }

        // For higher timeframes, calculate from parent
        TimeframeType parentTf = tf.getParent();
        if (parentTf == null) {
            return result;
        }

        int tfSeconds = tf.getSeconds();
        int parentSeconds = parentTf.getSeconds();
        int ratio = tfSeconds / parentSeconds;

        List<double[]> parentData = getTimeframeData(buffer, validHours, parentTf);

        int i = 0;
        while (i < parentData.size()) {
            long parentTs = (long) parentData.get(i)[0];
            long periodStart = (parentTs / tfSeconds) * tfSeconds;

            double sum = 0;
            int count = 0;

            while (i < parentData.size() && count < ratio) {
                long ts = (long) parentData.get(i)[0];
                long expectedPeriodStart = (ts / tfSeconds) * tfSeconds;

                if (expectedPeriodStart == periodStart) {
                    sum += parentData.get(i)[1];
                    count++;
                    i++;
                } else {
                    break;
                }
            }

            if (count == ratio) {
                double avg = sum / ratio;
                result.add(new double[] { periodStart, avg });
            } else if (count > 0) {
                i -= count;
                while (i < parentData.size()) {
                    long ts = (long) parentData.get(i)[0];
                    long nextPeriodStart = (ts / tfSeconds) * tfSeconds;
                    if (nextPeriodStart > periodStart) {
                        break;
                    }
                    i++;
                }
            }
        }

        return result;
    }

    /**
     * Get the instrument buffers (for external access after warmup).
     */
    public Map<Instrument, InstrumentBuffer> getInstrumentBuffers() {
        return instrumentBuffers;
    }

    /**
     * Get buffer for a specific instrument.
     */
    public InstrumentBuffer getBuffer(Instrument instrument) {
        return instrumentBuffers.get(instrument);
    }

    /**
     * Repair a gap in the data by fetching ticks for a specific time range.
     * Used after short disconnections to fill in missing data.
     *
     * @param fromEpoch Start of gap (epoch seconds)
     * @param toEpoch End of gap (epoch seconds)
     */
    public void repairGap(long fromEpoch, long toEpoch) throws Exception {
        System.out.println("[" + new Date() + "] Repairing gap from " + fromEpoch + " to " + toEpoch);

        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            Instrument instrument = entry.getKey();
            InstrumentBuffer buffer = entry.getValue();

            repairGapForInstrument(instrument, buffer, fromEpoch, toEpoch);
        }
    }

    /**
     * Repair gap for a single instrument.
     */
    private void repairGapForInstrument(Instrument instrument, InstrumentBuffer buffer,
                                        long fromEpoch, long toEpoch) throws Exception {
        long fromMs = fromEpoch * 1000;
        long toMs = toEpoch * 1000;

        // Fetch ticks for the gap period
        List<ITick> ticks = history.getTicks(instrument, fromMs, toMs);

        if (ticks == null || ticks.isEmpty()) {
            System.out.println("[" + new Date() + "] No ticks found for " + instrument + " during gap");
            return;
        }

        // Group ticks by second
        Map<Long, List<double[]>> ticksBySecond = new LinkedHashMap<>();
        for (ITick tick : ticks) {
            double bid = tick.getBid();
            double ask = tick.getAsk();
            if (!Double.isNaN(bid) && !Double.isNaN(ask)) {
                long epochSecond = tick.getTime() / 1000;
                ticksBySecond.computeIfAbsent(epochSecond, k -> new ArrayList<>())
                        .add(new double[] { (bid + ask) / 2.0, ask - bid });
            }
        }

        // Get last valid value for forward-filling
        TimeframeBuffer s1Buffer = buffer.getBuffer(TimeframeType.S1);
        double lastMid = s1Buffer != null ? s1Buffer.getLatestMid() : Double.NaN;
        double lastSpread = s1Buffer != null ? s1Buffer.getLatestSpread() : Double.NaN;

        // Process each second in the gap
        for (long epochSecond = fromEpoch; epochSecond <= toEpoch; epochSecond++) {
            List<double[]> secondTicks = ticksBySecond.get(epochSecond);

            double mid, spread;
            if (secondTicks != null && !secondTicks.isEmpty()) {
                // Average the ticks for this second
                double midSum = 0, spreadSum = 0;
                for (double[] t : secondTicks) {
                    midSum += t[0];
                    spreadSum += t[1];
                }
                mid = midSum / secondTicks.size();
                spread = spreadSum / secondTicks.size();
                lastMid = mid;
                lastSpread = spread;
            } else if (!Double.isNaN(lastMid) && lastMid >= 0) {
                // Forward-fill from last valid value
                mid = lastMid;
                spread = lastSpread;
            } else {
                // No valid value - skip this second (already has -1 from disconnect)
                continue;
            }

            // Update S1 buffer directly (we need to replace -1 values)
            // For now, just add the value - the buffer will overwrite if timestamp matches
            // Note: This is a simplified approach; full implementation would need buffer update
            buffer.addS1AndUpdateHigher(epochSecond, mid, spread);
        }

        System.out.println("[" + new Date() + "] Repaired " + ticksBySecond.size() +
                " seconds with ticks for " + instrument);
    }
}
