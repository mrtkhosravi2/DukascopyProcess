package com.dukascopy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {
    private final String username;
    private final String password;
    private final String jnlpUrl;
    private final List<String> instruments;
    private final String startingMonth;
    private final String outputDirectory;
    private final boolean startupDownloadEnabled;
    private final int startupDownloadWindowMinutes;

    public Config(String configPath) throws Exception {
        Map<String, String> props = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
            String currentSection = "";
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSection = line.substring(1, line.length() - 1) + ".";
                    continue;
                }
                int equalsIndex = line.indexOf('=');
                if (equalsIndex > 0) {
                    String key = currentSection + line.substring(0, equalsIndex).trim();
                    String value = line.substring(equalsIndex + 1).trim();
                    props.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new Exception("Failed to load config file: " + configPath, e);
        }

        username = getRequired(props, "jforex.username");
        password = getRequired(props, "jforex.password");
        jnlpUrl = getRequired(props, "jforex.jnlpUrl");

        String instrumentsStr = getRequired(props, "download.instruments");
        instruments = parseInstruments(instrumentsStr);

        startingMonth = getRequired(props, "download.starting_month");
        outputDirectory = props.getOrDefault("download.output_directory", "./processed-data");
        startupDownloadEnabled = Boolean.parseBoolean(
            props.getOrDefault("download.startup_download_enabled", "true")
        );
        startupDownloadWindowMinutes = Integer.parseInt(
            props.getOrDefault("download.startup_download_window_minutes", "120")
        );
    }

    private String getRequired(Map<String, String> props, String key) throws Exception {
        String value = props.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new Exception("Missing required configuration: " + key);
        }
        return value.trim();
    }

    private List<String> parseInstruments(String instrumentsStr) {
        List<String> result = new ArrayList<>();
        String[] parts = instrumentsStr.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getJnlpUrl() {
        return jnlpUrl;
    }

    public List<String> getInstruments() {
        return instruments;
    }

    public String getStartingMonth() {
        return startingMonth;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public boolean isStartupDownloadEnabled() {
        return startupDownloadEnabled;
    }

    public int getStartupDownloadWindowMinutes() {
        return startupDownloadWindowMinutes;
    }
}
