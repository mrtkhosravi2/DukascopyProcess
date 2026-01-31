package com.dukascopy.live.test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.TimeZone;

/**
 * Test client that connects to the live WebSocket and prints the last 10 values
 * for each timeframe whenever an update is received.
 *
 * Usage: java WebSocketTestClient [host] [port]
 *   - host: WebSocket server host (default: localhost)
 *   - port: WebSocket server port (default: 8444)
 */
public class WebSocketTestClient {

    private static final String[] TIMEFRAMES = {"S1", "S5", "S10", "S30", "M1", "M3", "M15", "H1"};
    private static final int DISPLAY_COUNT = 10;
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    static {
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("EET"));
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8444;

        System.out.println("[" + new Date() + "] Connecting to WebSocket at " + host + ":" + port);

        try (Socket socket = new Socket(host, port)) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            performHandshake(out, in, host, port);
            System.out.println("[" + new Date() + "] WebSocket handshake successful");

            while (true) {
                String message = readFrame(in);
                if (message != null) {
                    clearScreen();
                    System.out.println("[" + new Date() + "] Received update:");
                    System.out.println();
                    printSnapshot(message);
                }
            }
        } catch (Exception e) {
            System.err.println("[" + new Date() + "] Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void performHandshake(OutputStream out, InputStream in, String host, int port) throws Exception {
        byte[] keyBytes = new byte[16];
        new SecureRandom().nextBytes(keyBytes);
        String key = Base64.getEncoder().encodeToString(keyBytes);

        String request = "GET / HTTP/1.1\r\n" +
            "Host: " + host + ":" + port + "\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Key: " + key + "\r\n" +
            "Sec-WebSocket-Version: 13\r\n\r\n";

        out.write(request.getBytes(StandardCharsets.UTF_8));
        out.flush();

        byte[] buffer = new byte[4096];
        int len = in.read(buffer);
        if (len <= 0) {
            throw new Exception("No handshake response received");
        }

        String response = new String(buffer, 0, len, StandardCharsets.UTF_8);
        if (!response.contains("101")) {
            throw new Exception("Handshake failed: " + response);
        }
    }

    private static String readFrame(InputStream in) throws Exception {
        int b1 = in.read();
        if (b1 == -1) return null;

        int b2 = in.read();
        if (b2 == -1) return null;

        boolean masked = (b2 & 0x80) != 0;
        int payloadLen = b2 & 0x7F;

        if (payloadLen == 126) {
            int b3 = in.read();
            int b4 = in.read();
            payloadLen = ((b3 & 0xFF) << 8) | (b4 & 0xFF);
        } else if (payloadLen == 127) {
            long longLen = 0;
            for (int i = 0; i < 8; i++) {
                longLen = (longLen << 8) | (in.read() & 0xFF);
            }
            payloadLen = (int) longLen;
        }

        byte[] maskKey = null;
        if (masked) {
            maskKey = new byte[4];
            in.read(maskKey);
        }

        byte[] payload = new byte[payloadLen];
        int totalRead = 0;
        while (totalRead < payloadLen) {
            int read = in.read(payload, totalRead, payloadLen - totalRead);
            if (read == -1) break;
            totalRead += read;
        }

        if (masked && maskKey != null) {
            for (int i = 0; i < payload.length; i++) {
                payload[i] ^= maskKey[i % 4];
            }
        }

        return new String(payload, StandardCharsets.UTF_8);
    }

    private static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }

    private static void printSnapshot(String json) {
        try {
            boolean connected = extractBoolean(json, "connected");
            boolean hasNaN = extractBoolean(json, "hasNaN");
            boolean closedMarketDay = extractBoolean(json, "closedMarketDay");

            System.out.println("Status: connected=" + connected + ", hasNaN=" + hasNaN + ", closedMarketDay=" + closedMarketDay);
            System.out.println();

            String instrumentsJson = extractObject(json, "instruments");
            if (instrumentsJson == null) {
                System.out.println("No instruments data");
                return;
            }

            int idx = 0;
            while (idx < instrumentsJson.length()) {
                int keyStart = instrumentsJson.indexOf('"', idx);
                if (keyStart == -1) break;

                int keyEnd = instrumentsJson.indexOf('"', keyStart + 1);
                if (keyEnd == -1) break;

                String instrument = instrumentsJson.substring(keyStart + 1, keyEnd);

                int colonIdx = instrumentsJson.indexOf(':', keyEnd);
                if (colonIdx == -1) break;

                String instrumentData = extractObjectFrom(instrumentsJson, colonIdx + 1);
                if (instrumentData == null) break;

                printInstrument(instrument, instrumentData);
                System.out.println();

                idx = colonIdx + 1 + instrumentData.length() + 2;
            }

        } catch (Exception e) {
            System.err.println("Parse error: " + e.getMessage());
        }
    }

    private static void printInstrument(String instrument, String data) {
        System.out.println("=== " + instrument + " ===");
        System.out.println();

        for (String tf : TIMEFRAMES) {
            String tfData = extractObject(data, tf);
            if (tfData != null) {
                double[] midValues = extractDoubleArray(tfData, "mid");
                long[] timestamps = extractLongArray(tfData, "ts");
                double[] spreadValues = extractDoubleArray(tfData, "spread");

                System.out.println("  " + tf + ":");
                if (midValues != null && midValues.length > 0 && timestamps != null) {
                    printValuesWithTimestamps(midValues, timestamps, spreadValues, DISPLAY_COUNT);
                }
            }
        }
    }

    private static void printValuesWithTimestamps(double[] midValues, long[] timestamps, double[] spreadValues, int n) {
        int start = Math.max(0, midValues.length - n);

        System.out.print("    ");
        System.out.printf("%-10s %-14s", "Time(EET)", "Mid");
        if (spreadValues != null) {
            System.out.printf(" %-10s", "Spread");
        }
        System.out.println();
        System.out.print("    ");
        System.out.println("--------------------------------------");

        for (int i = start; i < midValues.length; i++) {
            System.out.print("    ");

            // Format timestamp
            if (timestamps[i] == -1) {
                System.out.printf("%-10s ", "null");
            } else {
                String timeStr = TIME_FORMAT.format(new Date(timestamps[i] * 1000));
                System.out.printf("%-10s ", timeStr);
            }

            // Format mid value
            if (midValues[i] == -1.0) {
                System.out.printf("%-14s", "null");
            } else {
                System.out.printf("%-14.5f", midValues[i]);
            }

            // Format spread value if present
            if (spreadValues != null && i < spreadValues.length) {
                if (spreadValues[i] == -1.0) {
                    System.out.printf(" %-10s", "null");
                } else {
                    System.out.printf(" %-10.5f", spreadValues[i]);
                }
            }

            System.out.println();
        }
    }

    private static boolean extractBoolean(String json, String key) {
        String pattern = "\"" + key + "\":";
        int idx = json.indexOf(pattern);
        if (idx == -1) return false;
        int start = idx + pattern.length();
        return json.substring(start).trim().startsWith("true");
    }

    private static String extractObject(String json, String key) {
        String pattern = "\"" + key + "\":";
        int idx = json.indexOf(pattern);
        if (idx == -1) return null;
        return extractObjectFrom(json, idx + pattern.length());
    }

    private static String extractObjectFrom(String json, int startIdx) {
        int braceStart = json.indexOf('{', startIdx);
        if (braceStart == -1) return null;

        int depth = 1;
        int i = braceStart + 1;
        while (i < json.length() && depth > 0) {
            char c = json.charAt(i);
            if (c == '{') depth++;
            else if (c == '}') depth--;
            i++;
        }

        return json.substring(braceStart, i);
    }

    private static double[] extractDoubleArray(String json, String key) {
        String pattern = "\"" + key + "\":";
        int idx = json.indexOf(pattern);
        if (idx == -1) return null;

        int bracketStart = json.indexOf('[', idx);
        if (bracketStart == -1) return null;

        int bracketEnd = json.indexOf(']', bracketStart);
        if (bracketEnd == -1) return null;

        String arrayStr = json.substring(bracketStart + 1, bracketEnd);
        if (arrayStr.trim().isEmpty()) return new double[0];

        String[] parts = arrayStr.split(",");
        double[] result = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.equals("null") || part.equals("-1")) {
                result[i] = -1.0;
            } else {
                result[i] = Double.parseDouble(part);
            }
        }
        return result;
    }

    private static long[] extractLongArray(String json, String key) {
        String pattern = "\"" + key + "\":";
        int idx = json.indexOf(pattern);
        if (idx == -1) return null;

        int bracketStart = json.indexOf('[', idx);
        if (bracketStart == -1) return null;

        int bracketEnd = json.indexOf(']', bracketStart);
        if (bracketEnd == -1) return null;

        String arrayStr = json.substring(bracketStart + 1, bracketEnd);
        if (arrayStr.trim().isEmpty()) return new long[0];

        String[] parts = arrayStr.split(",");
        long[] result = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.equals("null") || part.equals("-1")) {
                result[i] = -1L;
            } else {
                result[i] = Long.parseLong(part);
            }
        }
        return result;
    }
}
