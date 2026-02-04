package com.dukascopy.live;

import com.dukascopy.api.ITick;
import com.dukascopy.api.Instrument;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Central coordinator for live tick processing.
 * Subscribes to JForex tick stream, aggregates ticks per second,
 * and updates ring buffers.
 */
public class LiveDataProcessor {
    private static final TimeZone EET = TimeZone.getTimeZone("EET");
    private static final long GAP_REPAIR_THRESHOLD_SECONDS = 60;

    private final WarmupManager warmupManager;
    private final Map<Instrument, InstrumentBuffer> instrumentBuffers;
    private final Map<Instrument, SecondAggregator> aggregators;
    private final Map<Instrument, double[]> lastValidValues;  // [mid, spread]

    // Buffer backups for holiday detection rollback
    private Map<Instrument, InstrumentBuffer> bufferBackups;

    // State variables
    private volatile boolean connected = true;
    private volatile boolean closeMarketDay = false;
    private volatile long disconnectTimestamp = -1;
    private volatile boolean hasNaN = false;
    private volatile boolean ticksReceivedToday = false;

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private long lastProcessedSecond = -1;

    public LiveDataProcessor(WarmupManager warmupManager) {
        this.warmupManager = warmupManager;
        this.instrumentBuffers = warmupManager.getInstrumentBuffers();
        this.aggregators = new HashMap<>();
        this.lastValidValues = new HashMap<>();

        // Initialize aggregators for each instrument
        for (Instrument instrument : instrumentBuffers.keySet()) {
            aggregators.put(instrument, new SecondAggregator());
            lastValidValues.put(instrument, new double[] { Double.NaN, Double.NaN });
        }

        // Initialize last valid values from S1 buffer
        initializeLastValidValues();
    }

    /**
     * Initialize last valid values from the most recent S1 data in buffers.
     */
    private void initializeLastValidValues() {
        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            TimeframeBuffer s1Buffer = entry.getValue().getBuffer(TimeframeType.S1);
            if (s1Buffer != null && s1Buffer.getSize() > 0) {
                double lastMid = s1Buffer.getLatestMid();
                double lastSpread = s1Buffer.getLatestSpread();
                if (!Double.isNaN(lastMid) && lastMid >= 0) {
                    lastValidValues.put(entry.getKey(), new double[] { lastMid, lastSpread });
                }
            }
        }
    }

    /**
     * Start the second-boundary timer.
     */
    public void start() {
        if (running) return;
        running = true;

        // Check for NaN values in buffers
        recalculateHasNaN();

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LiveDataProcessor");
            t.setDaemon(true);
            return t;
        });

        // Schedule to run at next second boundary
        long now = System.currentTimeMillis();
        long nextSecond = ((now / 1000) + 1) * 1000;
        long initialDelay = nextSecond - now;

        scheduler.scheduleAtFixedRate(this::onSecondBoundary, initialDelay, 1000, TimeUnit.MILLISECONDS);

        System.out.println("[" + new Date() + "] LiveDataProcessor started");
    }

    /**
     * Stop the processor and cleanup.
     */
    public void stop() {
        running = false;

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }

        System.out.println("[" + new Date() + "] LiveDataProcessor stopped");
    }

    /**
     * Process an incoming tick from onTick().
     */
    public void processTick(Instrument instrument, ITick tick) {
        if (!running) return;

        SecondAggregator aggregator = aggregators.get(instrument);
        if (aggregator == null) return;

        double bid = tick.getBid();
        double ask = tick.getAsk();

        if (!Double.isNaN(bid) && !Double.isNaN(ask)) {
            double mid = (bid + ask) / 2.0;
            double spread = ask - bid;
            aggregator.addTick(mid, spread);
            ticksReceivedToday = true;
        }
    }

    /**
     * Called when connection to Dukascopy is lost.
     */
    public void onDisconnect() {
        if (!connected) return;  // Already disconnected

        connected = false;
        disconnectTimestamp = System.currentTimeMillis() / 1000;
        System.out.println("[" + new Date() + "] LiveDataProcessor: Disconnected at epoch " + disconnectTimestamp);
    }

    /**
     * Called when connection to Dukascopy is restored.
     */
    public void onReconnect() {
        if (connected) return;  // Already connected

        long currentEpoch = System.currentTimeMillis() / 1000;
        long gap = currentEpoch - disconnectTimestamp;

        System.out.println("[" + new Date() + "] LiveDataProcessor: Reconnected after " + gap + " seconds");

        if (gap < GAP_REPAIR_THRESHOLD_SECONDS) {
            // Small gap - repair via historical API
            try {
                warmupManager.repairGap(disconnectTimestamp, currentEpoch);
                recalculateHasNaN();
                System.out.println("[" + new Date() + "] Gap repair completed");
            } catch (Exception e) {
                System.err.println("[" + new Date() + "] Gap repair failed: " + e.getMessage());
                // Leave hasNaN true if repair failed
            }
        } else {
            // Large gap - full warmup restart
            try {
                System.out.println("[" + new Date() + "] Large gap detected, performing full warmup...");
                warmupManager.performWarmup();
                initializeLastValidValues();
                recalculateHasNaN();
                System.out.println("[" + new Date() + "] Full warmup completed");
            } catch (Exception e) {
                System.err.println("[" + new Date() + "] Full warmup failed: " + e.getMessage());
            }
        }

        connected = true;
        disconnectTimestamp = -1;
    }

    /**
     * Called at each second boundary to finalize the previous second's data.
     */
    private void onSecondBoundary() {
        try {
            long currentEpoch = System.currentTimeMillis() / 1000;
            long previousSecond = currentEpoch - 1;

            // Skip if we already processed this second
            if (previousSecond <= lastProcessedSecond) {
                return;
            }
            lastProcessedSecond = previousSecond;

            // Check for midnight/holiday transitions
            checkMidnight(currentEpoch);

            // Skip data updates if it's a close market day
            if (closeMarketDay) {
                return;
            }

            // Process each instrument
            for (Map.Entry<Instrument, SecondAggregator> entry : aggregators.entrySet()) {
                Instrument instrument = entry.getKey();
                SecondAggregator aggregator = entry.getValue();
                InstrumentBuffer buffer = instrumentBuffers.get(instrument);

                if (buffer == null) continue;

                SecondAggregator.AggregatedSecond result = aggregator.finalizeAndReset();

                double mid, spread;
                if (!connected) {
                    // Disconnected - insert -1 markers
                    mid = -1;
                    spread = -1;
                    hasNaN = true;
                } else if (result != null) {
                    // Connected with ticks - use averaged values
                    mid = result.mid;
                    spread = result.spread;
                    lastValidValues.put(instrument, new double[] { mid, spread });
                } else {
                    // Connected without ticks - forward-fill from last value
                    double[] lastValid = lastValidValues.get(instrument);
                    if (lastValid != null && !Double.isNaN(lastValid[0]) && lastValid[0] >= 0) {
                        mid = lastValid[0];
                        spread = lastValid[1];
                    } else {
                        // No valid previous value - use -1
                        mid = -1;
                        spread = -1;
                        hasNaN = true;
                    }
                }

                // Update buffers
                buffer.addS1AndUpdateHigher(previousSecond, mid, spread);
            }
        } catch (Exception e) {
            System.err.println("[" + new Date() + "] Error in onSecondBoundary: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Check for midnight transitions and handle backup/holiday detection.
     */
    private void checkMidnight(long epochSecond) {
        Calendar cal = Calendar.getInstance(EET);
        cal.setTimeInMillis(epochSecond * 1000);

        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);

        // At 00:00:00 EET - create backup and reset flags
        if (hour == 0 && minute == 0 && second == 0) {
            createBufferBackup();
            ticksReceivedToday = false;
            closeMarketDay = isWeekend(epochSecond);
            System.out.println("[" + new Date() + "] Midnight: backup created, closeMarketDay=" + closeMarketDay);
        }

        // At 02:00:00 EET - check for holiday
        if (hour == 2 && minute == 0 && second == 0) {
            checkHolidayAt0200();
        }
    }

    /**
     * Check if epoch second falls on a weekend (Friday 22:00 UTC to Sunday 22:00 UTC).
     */
    private boolean isWeekend(long epochSecond) {
        Calendar cal = Calendar.getInstance(EET);
        cal.setTimeInMillis(epochSecond * 1000);
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        return dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY;
    }

    /**
     * Check if it's a holiday at 02:00 EET.
     * If no ticks received since midnight and it's not already marked as closeMarketDay,
     * this is likely a holiday - restore buffer backup.
     */
    private void checkHolidayAt0200() {
        if (!closeMarketDay && !ticksReceivedToday) {
            System.out.println("[" + new Date() + "] Holiday detected at 02:00 - restoring buffer backup");
            restoreBufferBackup();
            closeMarketDay = true;
        }
    }

    /**
     * Create a backup of all instrument buffers.
     */
    private void createBufferBackup() {
        bufferBackups = new HashMap<>();
        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            bufferBackups.put(entry.getKey(), entry.getValue().copy());
        }
    }

    /**
     * Restore all instrument buffers from backup.
     */
    private void restoreBufferBackup() {
        if (bufferBackups == null) return;

        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            InstrumentBuffer backup = bufferBackups.get(entry.getKey());
            if (backup != null) {
                entry.getValue().copyFrom(backup);
            }
        }
    }

    /**
     * Recalculate hasNaN by checking all buffers for negative values.
     */
    private void recalculateHasNaN() {
        hasNaN = false;
        for (InstrumentBuffer buffer : instrumentBuffers.values()) {
            if (buffer.containsNegative()) {
                hasNaN = true;
                break;
            }
        }
    }

    // Getters for state

    public boolean isConnected() {
        return connected;
    }

    public boolean hasNaN() {
        return hasNaN;
    }

    public boolean isCloseMarketDay() {
        return closeMarketDay;
    }
}
