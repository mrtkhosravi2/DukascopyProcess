package com.dukascopy;

import com.dukascopy.api.IHistory;
import com.dukascopy.api.ITick;
import com.dukascopy.api.Instrument;

import java.io.File;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

public class MonthProcessor {
    private final IHistory history;
    private final Instrument instrument;
    private final YearMonth yearMonth;
    private final String outputDirectory;
    private final TickAggregator aggregator;
    private final SimpleDateFormat dateFormat;
    private final DecimalFormat numberFormat;

    public MonthProcessor(IHistory history, Instrument instrument, YearMonth yearMonth, String outputDirectory) {
        this.history = history;
        this.instrument = instrument;
        this.yearMonth = yearMonth;
        this.outputDirectory = outputDirectory;
        this.aggregator = new TickAggregator();

        this.dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
        this.dateFormat.setTimeZone(TimeZone.getTimeZone("EET"));

        this.numberFormat = new DecimalFormat("0.0000000");
    }

    public void process() throws Exception {
        String filename = getFilename();
        String completedFlag = filename + ".completed";
        File outputDir = new File(outputDirectory);

        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        File completedFile = new File(outputDir, completedFlag);
        if (completedFile.exists()) {
            return;
        }

        String baseFilename = filename.replace(".csv", "");
        System.out.println("============================================");
        System.out.println("[" + new Date() + "] " + baseFilename + " Download Started");

        long startTime = getMonthStartMillis();
        long endTime = getMonthEndMillis();

        List<ITick> ticks = history.getTicks(instrument, startTime, endTime);
        System.out.println("[" + new Date() + "] " + baseFilename + " Download Success");

        System.out.println("[" + new Date() + "] " + baseFilename + " Process Started");
        Map<Long, TickAggregator.AggregatedData> aggregated = aggregator.aggregate(ticks);
        Map<Long, TickAggregator.AggregatedData> filled = fillGaps(aggregated, startTime / 1000, endTime / 1000);
        writeCsv(filled, filename);

        // Only mark as completed if the month is in the past
        if (yearMonth.isBefore(YearMonth.now())) {
            completedFile.createNewFile();
        }

        System.out.println("[" + new Date() + "] " + baseFilename + " Process Success");
        System.out.println("============================================");
    }

    private String getFilename() {
        String instrumentName = instrument.name().replace("/", "");
        return instrumentName + "-" + yearMonth.toString() + ".csv";
    }

    private long getMonthStartMillis() {
        ZonedDateTime start = yearMonth.atDay(1).atStartOfDay(ZoneId.of("EET"));
        return start.toInstant().toEpochMilli();
    }

    private long getMonthEndMillis() {
        ZonedDateTime end = yearMonth.plusMonths(1).atDay(1).atStartOfDay(ZoneId.of("EET"));
        long endMillis = end.toInstant().toEpochMilli();
        // Cap at start of today (exclude today's data)
        ZonedDateTime todayStart = ZonedDateTime.now(ZoneId.of("EET")).toLocalDate().atStartOfDay(ZoneId.of("EET"));
        long todayStartMillis = todayStart.toInstant().toEpochMilli();
        return Math.min(endMillis, todayStartMillis);
    }

    private Map<Long, TickAggregator.AggregatedData> fillGaps(
            Map<Long, TickAggregator.AggregatedData> data,
            long monthStartSec,
            long monthEndSec) {

        if (data.isEmpty()) {
            return data;
        }

        Map<Long, TickAggregator.AggregatedData> filled = new TreeMap<>();
        List<Long> timestamps = new ArrayList<>(data.keySet());

        // 1. Backward fill from first tick to start of month
        long firstTimestamp = timestamps.get(0);
        TickAggregator.AggregatedData firstData = data.get(firstTimestamp);
        for (long sec = monthStartSec; sec < firstTimestamp; sec++) {
            filled.put(sec, firstData);
        }

        // 2. Forward fill all gaps
        TickAggregator.AggregatedData lastData = null;
        long lastTimestamp = firstTimestamp - 1;

        for (long timestamp : timestamps) {
            TickAggregator.AggregatedData currentData = data.get(timestamp);

            // Fill gap from last to current
            if (lastData != null) {
                for (long sec = lastTimestamp + 1; sec < timestamp; sec++) {
                    filled.put(sec, lastData);
                }
            }

            filled.put(timestamp, currentData);
            lastData = currentData;
            lastTimestamp = timestamp;
        }

        // 3. Forward fill from last tick to end of month
        if (lastData != null) {
            for (long sec = lastTimestamp + 1; sec < monthEndSec; sec++) {
                filled.put(sec, lastData);
            }
        }

        // 4. Remove days with only one value (no changes)
        return removeFlatDays(filled);
    }

    private Map<Long, TickAggregator.AggregatedData> removeFlatDays(Map<Long, TickAggregator.AggregatedData> data) {
        Map<Long, TickAggregator.AggregatedData> result = new TreeMap<>();
        TimeZone eet = TimeZone.getTimeZone("EET");
        Calendar cal = Calendar.getInstance(eet);

        // Group by day and check if values change
        Map<Integer, List<Long>> dayTimestamps = new TreeMap<>();
        for (long timestamp : data.keySet()) {
            cal.setTimeInMillis(timestamp * 1000);
            int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
            dayTimestamps.computeIfAbsent(dayOfMonth, k -> new ArrayList<>()).add(timestamp);
        }

        // Check each day
        for (Map.Entry<Integer, List<Long>> entry : dayTimestamps.entrySet()) {
            List<Long> timestamps = entry.getValue();

            // Check if all values are the same
            boolean hasChange = false;
            TickAggregator.AggregatedData firstValue = data.get(timestamps.get(0));
            for (long timestamp : timestamps) {
                TickAggregator.AggregatedData value = data.get(timestamp);
                if (value.mid != firstValue.mid || value.spread != firstValue.spread) {
                    hasChange = true;
                    break;
                }
            }

            // Only include day if values change
            if (hasChange) {
                for (long timestamp : timestamps) {
                    result.put(timestamp, data.get(timestamp));
                }
            }
        }

        return result;
    }

    private void writeCsv(Map<Long, TickAggregator.AggregatedData> data, String filename) throws Exception {
        File outputFile = new File(outputDirectory, filename);

        try (PrintWriter writer = new PrintWriter(outputFile)) {
            writer.println("Time (EET),Mid,Spread");

            for (Map.Entry<Long, TickAggregator.AggregatedData> entry : data.entrySet()) {
                long epochSecond = entry.getKey();
                TickAggregator.AggregatedData aggData = entry.getValue();

                String timeStr = dateFormat.format(new Date(epochSecond * 1000));
                String midStr = numberFormat.format(aggData.mid);
                String spreadStr = numberFormat.format(aggData.spread);

                writer.println(timeStr + "," + midStr + "," + spreadStr);
            }
        }
    }
}
