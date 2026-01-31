package com.dukascopy.live;

import com.dukascopy.api.IHistory;
import com.dukascopy.api.ITick;
import com.dukascopy.api.Instrument;
import com.dukascopy.api.OfferSide;
import com.dukascopy.Config;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LiveDataManager {
    private static final TimeZone EET = TimeZone.getTimeZone("EET");
    private static final long WARMUP_HOURS = 64;

    private final Config config;
    private final IHistory history;
    private final Map<Instrument, InstrumentBuffer> instrumentBuffers;
    private final ScheduledExecutorService scheduler;

    private final AtomicBoolean warming = new AtomicBoolean(true);
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final AtomicBoolean hasNaN = new AtomicBoolean(false);
    private final AtomicBoolean closedMarketDay = new AtomicBoolean(false);
    private final AtomicLong disconnectTime = new AtomicLong(-1L);
    private final AtomicLong lastTickTime = new AtomicLong(-1L);

    public LiveDataManager(Config config, IHistory history, List<Instrument> instruments) {
        this.config = config;
        this.history = history;
        this.instrumentBuffers = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LiveDataManager-Scheduler");
            t.setDaemon(true);
            return t;
        });

        for (Instrument instrument : instruments) {
            instrumentBuffers.put(instrument, new InstrumentBuffer(
                instrument.toString(),
                config.getLiveTimeframes(),
                config.getLookbackWindow()
            ));
        }
    }

    public void start() {
        System.out.println("[" + new Date() + "] LiveDataManager starting warmup...");
        warming.set(true);

        Thread warmupThread = new Thread(() -> {
            try {
                performWarmup();
                warming.set(false);
                System.out.println("[" + new Date() + "] LiveDataManager warmup complete");
                startSecondBoundaryScheduler();
            } catch (Exception e) {
                System.err.println("[" + new Date() + "] LiveDataManager warmup failed: " + e.getMessage());
                e.printStackTrace();
            }
        }, "LiveDataManager-Warmup");
        warmupThread.setDaemon(true);
        warmupThread.start();
    }

    private void performWarmup() throws Exception {
        // Use a time slightly in the past to avoid "to parameter > last tick" error
        long now = System.currentTimeMillis() - 1000;
        long startTime = now - (WARMUP_HOURS * 60 * 60 * 1000L);

        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            Instrument instrument = entry.getKey();
            InstrumentBuffer buffer = entry.getValue();

            System.out.println("[" + new Date() + "] Loading history for " + instrument + "...");

            List<ITick> ticks = history.getTicks(instrument, startTime, now);

            if (ticks != null && !ticks.isEmpty()) {
                for (ITick tick : ticks) {
                    long epochSecond = tick.getTime() / 1000;
                    double bid = tick.getBid();
                    double ask = tick.getAsk();
                    double mid = (bid + ask) / 2.0;
                    double spread = ask - bid;

                    buffer.addHistoricalTick(epochSecond, mid, spread);
                }
                buffer.finalizeAllPeriods();
                System.out.println("[" + new Date() + "] Loaded " + ticks.size() + " ticks for " + instrument);
            } else {
                System.out.println("[" + new Date() + "] No historical ticks for " + instrument);
            }
        }
    }

    private void startSecondBoundaryScheduler() {
        long now = System.currentTimeMillis();
        long nextSecond = ((now / 1000) + 1) * 1000;
        long initialDelay = nextSecond - now;

        scheduler.scheduleAtFixedRate(this::onSecondBoundary, initialDelay, 1000, TimeUnit.MILLISECONDS);
    }

    private void onSecondBoundary() {
        if (warming.get()) return;

        long epochSecond = System.currentTimeMillis() / 1000;

        if (isWeekend(epochSecond)) {
            return;
        }

        checkHolidayDetection(epochSecond);

        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            entry.getValue().finalizeSecond(epochSecond);
        }
    }

    private boolean isWeekend(long epochSecond) {
        Calendar cal = Calendar.getInstance(EET);
        cal.setTimeInMillis(epochSecond * 1000);

        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);

        if (dayOfWeek == Calendar.SATURDAY) {
            return true;
        }

        if (dayOfWeek == Calendar.SUNDAY) {
            return true;
        }

        if (dayOfWeek == Calendar.FRIDAY && hour == 23 && minute >= 59) {
            return true;
        }

        if (dayOfWeek == Calendar.MONDAY && hour < 0) {
            return true;
        }

        return false;
    }

    private void checkHolidayDetection(long epochSecond) {
        Calendar cal = Calendar.getInstance(EET);
        cal.setTimeInMillis(epochSecond * 1000);

        int hour = cal.get(Calendar.HOUR_OF_DAY);

        if (hour >= 0 && hour < 2) {
            long lastTick = lastTickTime.get();
            if (lastTick > 0) {
                long gap = epochSecond - lastTick;
                if (gap > 7200) {
                    closedMarketDay.set(true);
                }
            }
        } else if (hour >= 2) {
            closedMarketDay.set(false);
        }
    }

    public void onTick(Instrument instrument, ITick tick) {
        if (warming.get()) return;

        if (!connected.get()) {
            onReconnect();
        }

        long epochSecond = tick.getTime() / 1000;
        double bid = tick.getBid();
        double ask = tick.getAsk();

        if (Double.isNaN(bid) || Double.isNaN(ask)) {
            hasNaN.set(true);
            return;
        }

        hasNaN.set(false);
        lastTickTime.set(epochSecond);

        double mid = (bid + ask) / 2.0;
        double spread = ask - bid;

        InstrumentBuffer buffer = instrumentBuffers.get(instrument);
        if (buffer != null) {
            buffer.processTick(epochSecond, mid, spread);
        }
    }

    public void onDisconnect() {
        connected.set(false);
        disconnectTime.set(System.currentTimeMillis() / 1000);
        System.out.println("[" + new Date() + "] LiveDataManager: Disconnected");
    }

    public void onReconnect() {
        if (connected.compareAndSet(false, true)) {
            System.out.println("[" + new Date() + "] LiveDataManager: Reconnected, performing backfill...");
            long disconnectedAt = disconnectTime.get();
            if (disconnectedAt > 0) {
                performBackfill(disconnectedAt);
            }
            disconnectTime.set(-1L);
        }
    }

    private void performBackfill(long fromEpochSecond) {
        long now = System.currentTimeMillis();
        long fromTime = fromEpochSecond * 1000;

        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            Instrument instrument = entry.getKey();
            InstrumentBuffer buffer = entry.getValue();

            try {
                List<ITick> ticks = history.getTicks(instrument, fromTime, now);
                if (ticks != null && !ticks.isEmpty()) {
                    for (ITick tick : ticks) {
                        long epochSecond = tick.getTime() / 1000;
                        double bid = tick.getBid();
                        double ask = tick.getAsk();
                        double mid = (bid + ask) / 2.0;
                        double spread = ask - bid;

                        buffer.addHistoricalTick(epochSecond, mid, spread);
                    }
                    buffer.finalizeAllPeriods();
                    System.out.println("[" + new Date() + "] Backfilled " + ticks.size() + " ticks for " + instrument);
                }
            } catch (Exception e) {
                System.err.println("[" + new Date() + "] Backfill failed for " + instrument + ": " + e.getMessage());
            }
        }
    }

    public Map<String, Object> getFullSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();

        snapshot.put("connected", connected.get());
        snapshot.put("hasNaN", hasNaN.get());
        snapshot.put("closedMarketDay", closedMarketDay.get());

        Map<String, Object> instruments = new LinkedHashMap<>();
        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            instruments.put(entry.getKey().toString(), entry.getValue().toJsonMap());
        }
        snapshot.put("instruments", instruments);

        return snapshot;
    }

    public boolean isWarming() {
        return warming.get();
    }

    public boolean isConnected() {
        return connected.get();
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        System.out.println("[" + new Date() + "] LiveDataManager shutdown complete");
    }
}
