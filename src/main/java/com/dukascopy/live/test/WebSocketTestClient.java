package com.dukascopy.live.test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Date;

/**
 * Simple WebSocket test client for testing the LiveWebServer.
 *
 * Usage: java -cp target/dukascopy-process-1.0-SNAPSHOT.jar com.dukascopy.live.test.WebSocketTestClient [host] [port]
 *
 * Default: localhost:8443
 */
public class WebSocketTestClient {

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8443;

        System.out.println("[" + new Date() + "] Connecting to ws://" + host + ":" + port);

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(0); // No timeout for reading

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // Perform WebSocket handshake
            String wsKey = generateWebSocketKey();
            String handshake = "GET / HTTP/1.1\r\n" +
                    "Host: " + host + ":" + port + "\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                    "Sec-WebSocket-Version: 13\r\n" +
                    "\r\n";

            out.write(handshake.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // Read handshake response
            byte[] responseBuffer = new byte[4096];
            int responseLen = in.read(responseBuffer);
            String response = new String(responseBuffer, 0, responseLen, StandardCharsets.UTF_8);

            if (!response.contains("101 Switching Protocols")) {
                System.err.println("Handshake failed: " + response);
                return;
            }

            System.out.println("[" + new Date() + "] Connected! Waiting for messages...");
            System.out.println("Press Ctrl+C to exit\n");

            // Read WebSocket frames
            int messageCount = 0;
            while (true) {
                String message = readWebSocketFrame(in);
                if (message == null) {
                    System.out.println("[" + new Date() + "] Connection closed by server");
                    break;
                }

                messageCount++;

                // Pretty print: show timestamp and truncated message
                String preview = message.length() > 200
                        ? message.substring(0, 200) + "..."
                        : message;

                System.out.println("[" + new Date() + "] Message #" + messageCount + " (" + message.length() + " bytes):");
                System.out.println(preview);
                System.out.println();
            }

        } catch (Exception e) {
            System.err.println("[" + new Date() + "] Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String generateWebSocketKey() {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static String readWebSocketFrame(InputStream in) throws Exception {
        // Read first two bytes (FIN/opcode and mask/length)
        int b1 = in.read();
        if (b1 == -1) return null;

        int b2 = in.read();
        if (b2 == -1) return null;

        // Check if it's a text frame (opcode 0x1) or close frame (0x8)
        int opcode = b1 & 0x0F;
        if (opcode == 0x8) {
            return null; // Close frame
        }

        // Get payload length
        int payloadLength = b2 & 0x7F;

        if (payloadLength == 126) {
            // Extended 16-bit length
            int b3 = in.read();
            int b4 = in.read();
            payloadLength = (b3 << 8) | b4;
        } else if (payloadLength == 127) {
            // Extended 64-bit length
            long length = 0;
            for (int i = 0; i < 8; i++) {
                length = (length << 8) | in.read();
            }
            payloadLength = (int) length;
        }

        // Read payload
        byte[] payload = new byte[payloadLength];
        int totalRead = 0;
        while (totalRead < payloadLength) {
            int read = in.read(payload, totalRead, payloadLength - totalRead);
            if (read == -1) return null;
            totalRead += read;
        }

        return new String(payload, StandardCharsets.UTF_8);
    }
}
