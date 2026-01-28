# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DukascopyProcess is a Java application that downloads historical forex tick data from Dukascopy's JForex platform, aggregates it per-second, and exports it as CSV files. It runs on a scheduled basis (daily at 00:10 EET) with an optional startup download window.

## Build and Run Commands

```bash
# Build the uber JAR with all dependencies
mvn clean package

# Run the application
java -jar target/dukascopy-process-1.0-SNAPSHOT.jar

# Or use the batch script (Windows)
run.bat

# Run CSV comparison utility
java -cp target/dukascopy-process-1.0-SNAPSHOT.jar com.dukascopy.CsvComparator <generated.csv> <ground_truth.csv>
```

## Configuration

Edit `config.ini` before running:
- `[jforex]` section: Dukascopy credentials and platform URL
- `[download]` section: instruments (comma-separated), starting_month (YYYY-MM format), output_directory

## Architecture

**Entry Point**: `Main.java` - Connects to JForex, implements `IStrategy` interface, schedules daily downloads

**Core Processing Pipeline**:
1. `DownloadTask` - Orchestrates downloads within time windows, iterates through months
2. `MonthProcessor` - Retrieves ticks for a month, aggregates per-second, fills gaps, removes flat days, exports CSV
3. `TickAggregator` - Groups ticks by second, calculates average mid and spread

**Key Patterns**:
- Uses `.completed` flag files to prevent reprocessing past months
- Gap filling: backward fill from month start, forward fill for gaps and to month end
- Removes "flat days" (no price changes) from output
- All timestamps use EET timezone

**CSV Output Format**: `Time (EET),Mid,Spread` with 7 decimal precision

## Dependencies

- JForex-API (v2.13.99) - Dukascopy trading API
- DDS2-jClient-JForex (v3.6.51) - Historical tick data client
- Java 11, Maven 3
