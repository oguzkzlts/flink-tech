package org.example.flink.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.flink.config.FlinkJobConfig;
import org.example.flink.models.AggregatedResult;
import org.example.flink.models.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EnhancedAggregationFunction extends RichWindowFunction<PriceEvent, AggregatedResult, String, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedAggregationFunction.class);

    private transient Counter windowCounter;
    private transient Counter alertCounter;
    private transient Counter anomalyCounter;
    private transient Meter eventsPerSecond;
    
    private transient FlinkJobConfig config;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        windowCounter = getRuntimeContext().getMetricGroup().counter("windows.processed");
        alertCounter = getRuntimeContext().getMetricGroup().counter("alerts.generated");
        anomalyCounter = getRuntimeContext().getMetricGroup().counter("anomalies.detected");
        eventsPerSecond = getRuntimeContext().getMetricGroup().meter("events.per.second", new MeterView(60));
        
        Object configObj = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap().get("job.config");
        if (configObj instanceof FlinkJobConfig) {
            this.config = (FlinkJobConfig) configObj;
        } else {
            this.config = new FlinkJobConfig();
        }
    }

    @Override
    public void apply(String key, TimeWindow window, Iterable<PriceEvent> input, Collector<AggregatedResult> out) throws Exception {
        windowCounter.inc();
        
        String[] keyParts = key.split("\\|");
        String symbol = keyParts[0];
        String source = keyParts[1];

        List<Double> prices = new ArrayList<>();
        double sum = 0;
        double minPrice = Double.MAX_VALUE;
        double maxPrice = Double.MIN_VALUE;
        double totalVolume = 0;
        long count = 0;

        for (PriceEvent event : input) {
            if (event.getPrice() != null) {
                double price = event.getPrice();
                prices.add(price);
                sum += price;
                minPrice = Math.min(minPrice, price);
                maxPrice = Math.max(maxPrice, price);
                eventsPerSecond.markEvent();
            }
            if (event.getVolume() != null) {
                totalVolume += event.getVolume();
            }
            count++;
        }

        if (count == 0) {
            LOG.warn("Empty window for key: {}", key);
            return;
        }

        double avgPrice = sum / count;
        double stdDev = calculateStdDev(prices, avgPrice);

        AggregatedResult result = new AggregatedResult();
        result.setSymbol(symbol);
        result.setSource(source);
        result.setAvgPrice(avgPrice);
        result.setMinPrice(minPrice == Double.MAX_VALUE ? 0 : minPrice);
        result.setMaxPrice(maxPrice == Double.MIN_VALUE ? 0 : maxPrice);
        result.setStdDev(stdDev);
        result.setCount(count);
        result.setTotalVolume(totalVolume);
        result.setWindowStart(window.getStart());
        result.setWindowEnd(window.getEnd());

        determineAlertLevel(result, symbol, avgPrice, stdDev, count);

        LOG.info("Window aggregated: {}", result);
        out.collect(result);
    }

    private double calculateStdDev(List<Double> values, double mean) {
        if (values.size() < 2) {
            return 0.0;
        }
        
        double sumSquaredDiff = 0;
        for (double value : values) {
            double diff = value - mean;
            sumSquaredDiff += diff * diff;
        }
        
        return Math.sqrt(sumSquaredDiff / values.size());
    }

    private void determineAlertLevel(AggregatedResult result, String symbol, double avgPrice, double stdDev, long count) {
        double threshold = org.example.flink.utils.ThresholdUtil.getThreshold(symbol);
        double alertThreshold = config.getAlertThresholdPercentage();
        double anomalyMultiplier = config.getAnomalyStdDevMultiplier();

        if (avgPrice > threshold) {
            result.setAlertLevel(AggregatedResult.AlertLevel.CRITICAL);
            result.setAlertReason(String.format("Price %.2f exceeds threshold %.2f (%.1f%% above)", 
                    avgPrice, threshold, ((avgPrice - threshold) / threshold) * 100));
            alertCounter.inc();
        } else if (stdDev > 0 && stdDev > (avgPrice * anomalyMultiplier / 100)) {
            result.setAlertLevel(AggregatedResult.AlertLevel.ANOMALY);
            result.setAlertReason(String.format("High volatility detected: stddev %.2f (%.1f%% of avg)", 
                    stdDev, (stdDev / avgPrice) * 100));
            anomalyCounter.inc();
        } else if (count > 0 && avgPrice > (threshold * (1 - alertThreshold / 100))) {
            result.setAlertLevel(AggregatedResult.AlertLevel.WARNING);
            result.setAlertReason(String.format("Price approaching threshold: %.2f vs %.2f", 
                    avgPrice, threshold));
            alertCounter.inc();
        } else {
            result.setAlertLevel(AggregatedResult.AlertLevel.NORMAL);
            result.setAlertReason("Within normal parameters");
        }
    }
}
