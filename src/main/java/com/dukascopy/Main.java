package com.dukascopy;

import com.dukascopy.api.IContext;
import com.dukascopy.api.IHistory;
import com.dukascopy.api.Instrument;
import com.dukascopy.api.system.IClient;
import com.dukascopy.api.system.ClientFactory;
import com.dukascopy.api.system.ISystemListener;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
    private static IClient client;
    private static Config config;
    private static volatile IContext context;

    public static void main(String[] args) {
        try {
            System.out.println("[" + new Date() + "] Starting Dukascopy Data Downloader");

            config = new Config("config.ini");
            System.out.println("[" + new Date() + "] Configuration loaded");

            System.out.println("[" + new Date() + "] Connecting to JForex...");
            client = ClientFactory.getDefaultInstance();
            client.setSystemListener(new SimpleSystemListener());

            client.connect(config.getJnlpUrl(), config.getUsername(), config.getPassword());

            int retries = 0;
            while (!client.isConnected() && retries < 30) {
                Thread.sleep(1000);
                retries++;
            }

            if (!client.isConnected()) {
                System.err.println("[" + new Date() + "] Failed to connect to JForex");
                System.exit(1);
            }

            System.out.println("[" + new Date() + "] Connected to JForex");

            client.startStrategy(new DataDownloadStrategy());

            retries = 0;
            while (context == null && retries < 30) {
                Thread.sleep(1000);
                retries++;
            }

            if (context == null) {
                System.err.println("[" + new Date() + "] Failed to start strategy");
                System.exit(1);
            }

            System.out.println("[" + new Date() + "] Strategy started, context available");

            IHistory history = context.getHistory();
            DownloadTask downloadTask = new DownloadTask(history, config);

            if (config.isStartupDownloadEnabled()) {
                System.out.println("[" + new Date() + "] Performing startup download...");
                long windowDurationMillis = config.getStartupDownloadWindowMinutes() * 60 * 1000L;
                long startupWindowEnd = System.currentTimeMillis() + windowDurationMillis;
                downloadTask.execute(startupWindowEnd);
                System.out.println("[" + new Date() + "] Startup download complete");
            }

            scheduleDaily(downloadTask);

            System.out.println("[" + new Date() + "] Scheduler started. Waiting for daily download window (00:10-00:40 EET)...");

            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("[" + new Date() + "] Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void scheduleDaily(DownloadTask downloadTask) {
        Timer timer = new Timer("DailyDownloader", true);

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("EET"));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 10);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        if (cal.getTimeInMillis() <= System.currentTimeMillis()) {
            cal.add(Calendar.DAY_OF_MONTH, 1);
        }

        System.out.println("[" + new Date() + "] Next download scheduled for: " + cal.getTime());

        long dayInMillis = 24 * 60 * 60 * 1000L;

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long windowEnd = System.currentTimeMillis() + (30 * 60 * 1000L);
                downloadTask.execute(windowEnd);
            }
        }, cal.getTime(), dayInMillis);
    }

    private static class SimpleSystemListener implements ISystemListener {
        @Override
        public void onStart(long processId) {
            System.out.println("[" + new Date() + "] Strategy started");
        }

        @Override
        public void onStop(long processId) {
            System.out.println("[" + new Date() + "] Strategy stopped");
        }

        @Override
        public void onConnect() {
            System.out.println("[" + new Date() + "] Connected");
        }

        @Override
        public void onDisconnect() {
            System.err.println("[" + new Date() + "] Disconnected");
        }
    }

    private static class DataDownloadStrategy implements com.dukascopy.api.IStrategy {
        @Override
        public void onStart(com.dukascopy.api.IContext ctx) throws com.dukascopy.api.JFException {
            Main.context = ctx;

            // Subscribe to instruments
            java.util.Set<Instrument> instruments = new java.util.HashSet<>();
            for (String instrumentStr : config.getInstruments()) {
                try {
                    String normalized = instrumentStr.replace("/", "");
                    Instrument instrument = Instrument.valueOf(normalized);
                    instruments.add(instrument);
                } catch (Exception e) {
                    System.err.println("[" + new Date() + "] Warning: Invalid instrument " + instrumentStr + ": " + e.getMessage());
                }
            }

            if (!instruments.isEmpty()) {
                ctx.setSubscribedInstruments(instruments, true);
                System.out.println("[" + new Date() + "] Subscribed to instruments: " + instruments);
            }

            System.out.println("[" + new Date() + "] DataDownloadStrategy started");
        }

        @Override
        public void onTick(com.dukascopy.api.Instrument instrument, com.dukascopy.api.ITick tick) {
        }

        @Override
        public void onBar(com.dukascopy.api.Instrument instrument, com.dukascopy.api.Period period,
                          com.dukascopy.api.IBar askBar, com.dukascopy.api.IBar bidBar) {
        }

        @Override
        public void onMessage(com.dukascopy.api.IMessage message) {
        }

        @Override
        public void onAccount(com.dukascopy.api.IAccount account) {
        }

        @Override
        public void onStop() {
        }
    }
}
