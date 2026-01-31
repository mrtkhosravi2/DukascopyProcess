package com.dukascopy;

import com.dukascopy.api.IHistory;
import com.dukascopy.api.Instrument;

import java.time.YearMonth;
import java.util.Date;

public class DownloadTask {
    private final IHistory history;
    private final Config config;

    public DownloadTask(IHistory history, Config config) {
        this.history = history;
        this.config = config;
    }

    public void execute(long windowEndMillis) {
        System.out.println("[" + new Date() + "] Starting download window");

        try {
            YearMonth start = YearMonth.parse(config.getStartingMonth());
            YearMonth current = YearMonth.now();

            for (YearMonth ym = start; !ym.isAfter(current); ym = ym.plusMonths(1)) {
                for (String instrumentStr : config.getInstruments()) {
                    Instrument instrument = parseInstrument(instrumentStr);

                    boolean success = false;
                    while (!success && System.currentTimeMillis() < windowEndMillis) {
                        try {
                            MonthProcessor processor = new MonthProcessor(
                                    history,
                                    instrument,
                                    ym,
                                    config.getOutputDirectory()
                            );
                            processor.process();
                            success = true;
                        } catch (Exception e) {
                            System.err.println("[" + new Date() + "] Error processing " +
                                    instrumentStr + "-" + ym + ": " + e.getMessage());

                            if (System.currentTimeMillis() < windowEndMillis) {
                                System.out.println("[" + new Date() + "] Retrying in 1 minute...");
                                try {
                                    Thread.sleep(60000);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                        }
                    }

                    if (!success) {
                        System.err.println("[" + new Date() + "] Failed to process " +
                                instrumentStr + "-" + ym + " (window closed)");
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[" + new Date() + "] Download task error: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[" + new Date() + "] Download window complete");
    }

    private Instrument parseInstrument(String instrumentStr) throws Exception {
        String normalized = instrumentStr.replace("/", "");
        try {
            Instrument instrument = Instrument.valueOf(normalized);
            if (instrument == null) {
                throw new Exception("Invalid instrument: " + instrumentStr + " (valueOf returned null)");
            }
            return instrument;
        } catch (IllegalArgumentException e) {
            throw new Exception("Invalid instrument: " + instrumentStr + " (no constant " + normalized + ")", e);
        } catch (Exception e) {
            throw new Exception("Invalid instrument: " + instrumentStr, e);
        }
    }
}
