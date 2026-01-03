package com.dukascopy;

import com.dukascopy.api.ITick;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TickAggregator {

    public static class AggregatedData {
        public final double mid;
        public final double spread;

        public AggregatedData(double mid, double spread) {
            this.mid = mid;
            this.spread = spread;
        }
    }

    public Map<Long, AggregatedData> aggregate(List<ITick> ticks) {
        Map<Long, List<TickData>> secondsMap = new TreeMap<>();

        for (ITick tick : ticks) {
            long epochSecond = tick.getTime() / 1000;
            double bid = tick.getBid();
            double ask = tick.getAsk();
            double mid = (bid + ask) / 2.0;
            double spread = ask - bid;

            TickData tickData = new TickData(mid, spread);
            secondsMap.computeIfAbsent(epochSecond, k -> new ArrayList<>()).add(tickData);
        }

        Map<Long, AggregatedData> result = new TreeMap<>();
        for (Map.Entry<Long, List<TickData>> entry : secondsMap.entrySet()) {
            long epochSecond = entry.getKey();
            List<TickData> ticksInSecond = entry.getValue();

            double sumMid = 0.0;
            double sumSpread = 0.0;
            for (TickData td : ticksInSecond) {
                sumMid += td.mid;
                sumSpread += td.spread;
            }

            double avgMid = sumMid / ticksInSecond.size();
            double avgSpread = sumSpread / ticksInSecond.size();

            result.put(epochSecond, new AggregatedData(avgMid, avgSpread));
        }

        return result;
    }

    private static class TickData {
        final double mid;
        final double spread;

        TickData(double mid, double spread) {
            this.mid = mid;
            this.spread = spread;
        }
    }
}
