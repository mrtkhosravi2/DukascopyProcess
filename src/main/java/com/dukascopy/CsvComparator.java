package com.dukascopy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class CsvComparator {

    private static class CsvRow {
        final String timestamp;
        final double mid;
        final double spread;

        CsvRow(String timestamp, double mid, double spread) {
            this.timestamp = timestamp;
            this.mid = mid;
            this.spread = spread;
        }
    }

    private static class ValueDiff {
        final String timestamp;
        final double generatedValue;
        final double groundTruthValue;
        final double diff;

        ValueDiff(String timestamp, double generatedValue, double groundTruthValue) {
            this.timestamp = timestamp;
            this.generatedValue = generatedValue;
            this.groundTruthValue = groundTruthValue;
            this.diff = Math.abs(generatedValue - groundTruthValue);
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java CsvComparator <generated_file> <ground_truth_file>");
            System.out.println();
            System.out.println("Example:");
            System.out.println("  java CsvComparator \"processed-data/EURUSD-2025-09.csv\" \"D:/DL/Dukascopy/1 month/processed-data/EURUSD_S1.csv\"");
            System.exit(1);
        }

        String generatedFile = args[0];
        String groundTruthFile = args[1];

        try {
            compareFiles(generatedFile, groundTruthFile, 1e-7);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Map<String, CsvRow> parseCsv(String filepath) throws Exception {
        Map<String, CsvRow> data = new LinkedHashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new Exception("Empty file: " + filepath);
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 3) {
                    continue;
                }

                String timestamp = parts[0];
                double mid = Double.parseDouble(parts[1]);
                double spread = Double.parseDouble(parts[2]);

                data.put(timestamp, new CsvRow(timestamp, mid, spread));
            }
        }

        return data;
    }

    private static void compareFiles(String generatedFile, String groundTruthFile, double tolerance) throws Exception {
        System.out.println("Loading generated file: " + generatedFile);
        Map<String, CsvRow> generated = parseCsv(generatedFile);

        System.out.println("Loading ground truth file: " + groundTruthFile);
        Map<String, CsvRow> groundTruth = parseCsv(groundTruthFile);

        System.out.println();
        System.out.println("================================================================================");
        System.out.println("COMPARISON REPORT");
        System.out.println("================================================================================");
        System.out.println();

        // Basic stats
        System.out.printf("Generated file rows: %,d%n", generated.size());
        System.out.printf("Ground truth rows:   %,d%n", groundTruth.size());
        System.out.printf("Difference:          %,d%n%n", generated.size() - groundTruth.size());

        // Find differences in timestamps
        Set<String> onlyInGenerated = new TreeSet<>(generated.keySet());
        onlyInGenerated.removeAll(groundTruth.keySet());

        Set<String> onlyInGroundTruth = new TreeSet<>(groundTruth.keySet());
        onlyInGroundTruth.removeAll(generated.keySet());

        Set<String> common = new TreeSet<>(generated.keySet());
        common.retainAll(groundTruth.keySet());

        // Report missing/extra timestamps
        if (!onlyInGenerated.isEmpty()) {
            System.out.printf("⚠ Timestamps only in GENERATED file: %,d%n", onlyInGenerated.size());
            int count = 0;
            if (onlyInGenerated.size() <= 20) {
                System.out.println("  All timestamps:");
            } else {
                System.out.println("  First 20:");
            }
            for (String ts : onlyInGenerated) {
                System.out.println("  - " + ts);
                if (++count >= 20) {
                    if (onlyInGenerated.size() > 20) {
                        System.out.printf("  ... and %,d more%n", onlyInGenerated.size() - 20);
                    }
                    break;
                }
            }
            System.out.println();
        }

        if (!onlyInGroundTruth.isEmpty()) {
            System.out.printf("⚠ Timestamps only in GROUND TRUTH file: %,d%n", onlyInGroundTruth.size());
            int count = 0;
            if (onlyInGroundTruth.size() <= 20) {
                System.out.println("  All timestamps:");
            } else {
                System.out.println("  First 20:");
            }
            for (String ts : onlyInGroundTruth) {
                System.out.println("  - " + ts);
                if (++count >= 20) {
                    if (onlyInGroundTruth.size() > 20) {
                        System.out.printf("  ... and %,d more%n", onlyInGroundTruth.size() - 20);
                    }
                    break;
                }
            }
            System.out.println();
        }

        // Compare values for common timestamps
        if (!common.isEmpty()) {
            System.out.printf("Common timestamps: %,d%n%n", common.size());

            List<ValueDiff> midDiffs = new ArrayList<>();
            List<ValueDiff> spreadDiffs = new ArrayList<>();

            for (String ts : common) {
                CsvRow genRow = generated.get(ts);
                CsvRow gtRow = groundTruth.get(ts);

                double midDiff = Math.abs(genRow.mid - gtRow.mid);
                double spreadDiff = Math.abs(genRow.spread - gtRow.spread);

                if (midDiff > tolerance) {
                    midDiffs.add(new ValueDiff(ts, genRow.mid, gtRow.mid));
                }

                if (spreadDiff > tolerance) {
                    spreadDiffs.add(new ValueDiff(ts, genRow.spread, gtRow.spread));
                }
            }

            // Report value differences
            if (!midDiffs.isEmpty()) {
                System.out.printf("⚠ Mid value differences (tolerance=%e): %,d%n", tolerance, midDiffs.size());
                System.out.println("  First 10:");
                for (int i = 0; i < Math.min(10, midDiffs.size()); i++) {
                    ValueDiff vd = midDiffs.get(i);
                    System.out.printf("  - %s: generated=%.7f, truth=%.7f, diff=%.10f%n",
                            vd.timestamp, vd.generatedValue, vd.groundTruthValue, vd.diff);
                }
                if (midDiffs.size() > 10) {
                    System.out.printf("  ... and %,d more%n", midDiffs.size() - 10);
                }
                System.out.println();
            } else {
                System.out.printf("✓ All Mid values match within tolerance (%e)%n%n", tolerance);
            }

            if (!spreadDiffs.isEmpty()) {
                System.out.printf("⚠ Spread value differences (tolerance=%e): %,d%n", tolerance, spreadDiffs.size());
                System.out.println("  First 10:");
                for (int i = 0; i < Math.min(10, spreadDiffs.size()); i++) {
                    ValueDiff vd = spreadDiffs.get(i);
                    System.out.printf("  - %s: generated=%.7f, truth=%.7f, diff=%.10f%n",
                            vd.timestamp, vd.generatedValue, vd.groundTruthValue, vd.diff);
                }
                if (spreadDiffs.size() > 10) {
                    System.out.printf("  ... and %,d more%n", spreadDiffs.size() - 10);
                }
                System.out.println();
            } else {
                System.out.printf("✓ All Spread values match within tolerance (%e)%n%n", tolerance);
            }

            // Summary
            System.out.println("================================================================================");
            System.out.println("SUMMARY");
            System.out.println("================================================================================");
            System.out.println();

            if (onlyInGenerated.isEmpty() && onlyInGroundTruth.isEmpty() &&
                midDiffs.isEmpty() && spreadDiffs.isEmpty()) {
                System.out.println("✓ FILES ARE IDENTICAL (within tolerance)");
            } else {
                System.out.println("⚠ DIFFERENCES FOUND:");
                if (!onlyInGenerated.isEmpty()) {
                    System.out.printf("  - %,d extra timestamps in generated%n", onlyInGenerated.size());
                }
                if (!onlyInGroundTruth.isEmpty()) {
                    System.out.printf("  - %,d missing timestamps in generated%n", onlyInGroundTruth.size());
                }
                if (!midDiffs.isEmpty()) {
                    System.out.printf("  - %,d Mid value mismatches%n", midDiffs.size());
                }
                if (!spreadDiffs.isEmpty()) {
                    System.out.printf("  - %,d Spread value mismatches%n", spreadDiffs.size());
                }
            }
            System.out.println();
        }
    }
}
