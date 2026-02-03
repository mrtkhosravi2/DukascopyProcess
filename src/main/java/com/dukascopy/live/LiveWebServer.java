package com.dukascopy.live;

import com.dukascopy.api.Instrument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WebSocket server that broadcasts ring buffer data to connected clients every second.
 */
public class LiveWebServer {
    private final Map<Instrument, InstrumentBuffer> instrumentBuffers;
    private final int port;

    private ServerSocket serverSocket;
    private final List<WebSocketClient> clients = new CopyOnWriteArrayList<>();
    private volatile boolean running = false;
    private Thread acceptThread;
    private ScheduledExecutorService broadcastScheduler;

    private final AtomicBoolean warming = new AtomicBoolean(true);

    public LiveWebServer(Map<Instrument, InstrumentBuffer> instrumentBuffers, int port) {
        this.instrumentBuffers = instrumentBuffers;
        this.port = port;
    }

    /**
     * Start the WebSocket server.
     */
    public void start() throws IOException {
        running = true;

        serverSocket = new ServerSocket(port);
        acceptThread = new Thread(this::acceptConnections, "WebSocket-Accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        System.out.println("[" + new Date() + "] WebSocket server started on port " + port);

        // Schedule broadcasting every second, aligned to second boundary
        broadcastScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "WebSocket-Broadcast");
            t.setDaemon(true);
            return t;
        });

        long now = System.currentTimeMillis();
        long nextSecond = ((now / 1000) + 1) * 1000;
        long initialDelay = nextSecond - now;

        broadcastScheduler.scheduleAtFixedRate(this::broadcastSnapshot, initialDelay, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * Set warming status. When warming, no data is broadcast.
     */
    public void setWarming(boolean warming) {
        this.warming.set(warming);
    }

    /**
     * Check if server is in warming state.
     */
    public boolean isWarming() {
        return warming.get();
    }

    private void acceptConnections() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                handleWebSocketHandshake(socket);
            } catch (IOException e) {
                if (running) {
                    System.err.println("[" + new Date() + "] WebSocket accept error: " + e.getMessage());
                }
            }
        }
    }

    private void handleWebSocketHandshake(Socket socket) {
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // Read HTTP upgrade request
            StringBuilder request = new StringBuilder();
            byte[] buffer = new byte[4096];
            int len = in.read(buffer);
            if (len > 0) {
                request.append(new String(buffer, 0, len, StandardCharsets.UTF_8));
            }

            String key = extractWebSocketKey(request.toString());
            if (key == null) {
                socket.close();
                return;
            }

            String acceptKey = generateAcceptKey(key);

            // Send upgrade response
            String response = "HTTP/1.1 101 Switching Protocols\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n";

            out.write(response.getBytes(StandardCharsets.UTF_8));
            out.flush();

            WebSocketClient client = new WebSocketClient(socket);
            clients.add(client);
            System.out.println("[" + new Date() + "] WebSocket client connected: " + socket.getRemoteSocketAddress());

        } catch (Exception e) {
            System.err.println("[" + new Date() + "] WebSocket handshake error: " + e.getMessage());
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private String extractWebSocketKey(String request) {
        for (String line : request.split("\r\n")) {
            if (line.toLowerCase().startsWith("sec-websocket-key:")) {
                return line.substring(18).trim();
            }
        }
        return null;
    }

    private String generateAcceptKey(String key) throws Exception {
        String magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] hash = sha1.digest((key + magic).getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }

    private void broadcastSnapshot() {
        if (warming.get() || clients.isEmpty()) {
            return;
        }

        Map<String, Object> snapshot = getFullSnapshot();
        String json = toJson(snapshot);
        byte[] frame = createWebSocketFrame(json);

        List<WebSocketClient> disconnected = new ArrayList<>();

        for (WebSocketClient client : clients) {
            try {
                client.send(frame);
            } catch (IOException e) {
                disconnected.add(client);
            }
        }

        for (WebSocketClient client : disconnected) {
            clients.remove(client);
            client.close();
            System.out.println("[" + new Date() + "] WebSocket client disconnected");
        }
    }

    /**
     * Get full snapshot of all instrument buffers.
     */
    public Map<String, Object> getFullSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();

        snapshot.put("warming", warming.get());
        snapshot.put("ts", formatTimestamp(System.currentTimeMillis()));

        Map<String, Object> instruments = new LinkedHashMap<>();
        for (Map.Entry<Instrument, InstrumentBuffer> entry : instrumentBuffers.entrySet()) {
            instruments.put(entry.getKey().toString(), entry.getValue().toJsonMap());
        }
        snapshot.put("instruments", instruments);

        return snapshot;
    }

    /**
     * Format milliseconds to "yyyy.MM.dd HH:mm:ss" in EET timezone.
     */
    private String formatTimestamp(long millis) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("EET"));
        return sdf.format(new Date(millis));
    }

    private byte[] createWebSocketFrame(String message) {
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        int length = messageBytes.length;

        byte[] frame;
        if (length < 126) {
            frame = new byte[2 + length];
            frame[0] = (byte) 0x81;  // FIN + text frame
            frame[1] = (byte) length;
            System.arraycopy(messageBytes, 0, frame, 2, length);
        } else if (length < 65536) {
            frame = new byte[4 + length];
            frame[0] = (byte) 0x81;
            frame[1] = (byte) 126;
            frame[2] = (byte) ((length >> 8) & 0xFF);
            frame[3] = (byte) (length & 0xFF);
            System.arraycopy(messageBytes, 0, frame, 4, length);
        } else {
            frame = new byte[10 + length];
            frame[0] = (byte) 0x81;
            frame[1] = (byte) 127;
            for (int i = 0; i < 8; i++) {
                frame[2 + i] = (byte) ((length >> (56 - i * 8)) & 0xFF);
            }
            System.arraycopy(messageBytes, 0, frame, 10, length);
        }

        return frame;
    }

    @SuppressWarnings("unchecked")
    private String toJson(Object obj) {
        if (obj == null) {
            return "null";
        }

        if (obj instanceof Boolean) {
            return obj.toString();
        }

        if (obj instanceof Number) {
            double d = ((Number) obj).doubleValue();
            if (Double.isNaN(d)) {
                return "null";
            }
            if (d == (long) d) {
                return String.valueOf((long) d);
            }
            return String.format("%.7f", d);
        }

        if (obj instanceof String) {
            return "\"" + escapeJson((String) obj) + "\"";
        }

        if (obj instanceof double[]) {
            double[] arr = (double[]) obj;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) sb.append(",");
                double d = arr[i];
                if (Double.isNaN(d)) {
                    sb.append("null");
                } else if (d == (long) d) {
                    sb.append((long) d);
                } else {
                    sb.append(String.format("%.7f", d));
                }
            }
            sb.append("]");
            return sb.toString();
        }

        if (obj instanceof long[]) {
            long[] arr = (long[]) obj;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(arr[i]);
            }
            sb.append("]");
            return sb.toString();
        }

        if (obj instanceof String[]) {
            String[] arr = (String[]) obj;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) sb.append(",");
                if (arr[i] == null) {
                    sb.append("null");
                } else {
                    sb.append("\"").append(escapeJson(arr[i])).append("\"");
                }
            }
            sb.append("]");
            return sb.toString();
        }

        if (obj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) obj;
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (!first) sb.append(",");
                first = false;
                sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
                sb.append(toJson(entry.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }

        if (obj instanceof List) {
            List<Object> list = (List<Object>) obj;
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object item : list) {
                if (!first) sb.append(",");
                first = false;
                sb.append(toJson(item));
            }
            sb.append("]");
            return sb.toString();
        }

        return "\"" + escapeJson(obj.toString()) + "\"";
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * Stop the WebSocket server.
     */
    public void stop() {
        running = false;

        if (broadcastScheduler != null) {
            broadcastScheduler.shutdown();
            try {
                if (!broadcastScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    broadcastScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                broadcastScheduler.shutdownNow();
            }
        }

        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }

        for (WebSocketClient client : clients) {
            client.close();
        }
        clients.clear();

        System.out.println("[" + new Date() + "] WebSocket server stopped");
    }

    /**
     * Get number of connected clients.
     */
    public int getClientCount() {
        return clients.size();
    }

    private static class WebSocketClient {
        private final Socket socket;
        private final OutputStream out;

        WebSocketClient(Socket socket) throws IOException {
            this.socket = socket;
            this.out = socket.getOutputStream();
        }

        synchronized void send(byte[] data) throws IOException {
            out.write(data);
            out.flush();
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
