package com.dukascopy.live;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LiveWebServer {
    private final LiveDataManager dataManager;
    private final int restPort;
    private final int wsPort;

    private HttpServer httpServer;
    private ServerSocket wsServerSocket;
    private final List<WebSocketClient> wsClients = new CopyOnWriteArrayList<>();
    private volatile boolean running = false;
    private Thread wsAcceptThread;
    private ScheduledExecutorService broadcastScheduler;

    public LiveWebServer(LiveDataManager dataManager, int restPort) {
        this.dataManager = dataManager;
        this.restPort = restPort;
        this.wsPort = restPort + 1;
    }

    public void start() throws IOException {
        running = true;

        httpServer = HttpServer.create(new InetSocketAddress(restPort), 0);
        httpServer.createContext("/live", new LiveHandler());
        httpServer.setExecutor(Executors.newFixedThreadPool(4));
        httpServer.start();
        System.out.println("[" + new Date() + "] REST server started on port " + restPort);

        wsServerSocket = new ServerSocket(wsPort);
        wsAcceptThread = new Thread(this::acceptWebSocketConnections, "WebSocket-Accept");
        wsAcceptThread.setDaemon(true);
        wsAcceptThread.start();
        System.out.println("[" + new Date() + "] WebSocket server started on port " + wsPort);

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

    private void acceptWebSocketConnections() {
        while (running) {
            try {
                Socket socket = wsServerSocket.accept();
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

            String response = "HTTP/1.1 101 Switching Protocols\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n";

            out.write(response.getBytes(StandardCharsets.UTF_8));
            out.flush();

            WebSocketClient client = new WebSocketClient(socket);
            wsClients.add(client);
            System.out.println("[" + new Date() + "] WebSocket client connected: " + socket.getRemoteSocketAddress());

        } catch (Exception e) {
            System.err.println("[" + new Date() + "] WebSocket handshake error: " + e.getMessage());
            try {
                socket.close();
            } catch (IOException ignored) {}
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
        if (dataManager.isWarming() || wsClients.isEmpty()) {
            return;
        }

        Map<String, Object> snapshot = dataManager.getFullSnapshot();
        String json = toJson(snapshot);
        byte[] frame = createWebSocketFrame(json);

        List<WebSocketClient> disconnected = new ArrayList<>();

        for (WebSocketClient client : wsClients) {
            try {
                client.send(frame);
            } catch (IOException e) {
                disconnected.add(client);
            }
        }

        for (WebSocketClient client : disconnected) {
            wsClients.remove(client);
            client.close();
            System.out.println("[" + new Date() + "] WebSocket client disconnected");
        }
    }

    private byte[] createWebSocketFrame(String message) {
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        int length = messageBytes.length;

        byte[] frame;
        if (length < 126) {
            frame = new byte[2 + length];
            frame[0] = (byte) 0x81;
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

    private class LiveHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();

            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            if ("OPTIONS".equalsIgnoreCase(method)) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"GET".equalsIgnoreCase(method)) {
                String error = "{\"error\":\"Method not allowed\"}";
                byte[] bytes = error.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(405, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
                return;
            }

            if (dataManager.isWarming()) {
                String error = "{\"error\":\"Service warming up\"}";
                byte[] bytes = error.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(503, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
                return;
            }

            Map<String, Object> snapshot = dataManager.getFullSnapshot();
            String json = toJson(snapshot);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
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
            if (d == -1.0) {
                return "-1";
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
                if (d == -1.0) {
                    sb.append("-1");
                } else if (d == (long) d) {
                    sb.append((long) d);
                } else {
                    sb.append(String.format("%.7f", d));
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

    public void stop() {
        running = false;

        if (httpServer != null) {
            httpServer.stop(0);
        }

        if (broadcastScheduler != null) {
            broadcastScheduler.shutdown();
        }

        if (wsServerSocket != null) {
            try {
                wsServerSocket.close();
            } catch (IOException ignored) {}
        }

        for (WebSocketClient client : wsClients) {
            client.close();
        }
        wsClients.clear();

        System.out.println("[" + new Date() + "] LiveWebServer stopped");
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
            } catch (IOException ignored) {}
        }
    }
}
