package org.example.flink.models;

import java.io.Serializable;

public class AggregatedResult implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String symbol;
    private String source;
    private double avgPrice;
    private double minPrice;
    private double maxPrice;
    private double stdDev;
    private long count;
    private double totalVolume;
    private long windowStart;
    private long windowEnd;
    private AlertLevel alertLevel;
    private String alertReason;

    public enum AlertLevel {
        NORMAL, WARNING, CRITICAL, ANOMALY
    }

    public AggregatedResult() {}

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    
    public double getAvgPrice() { return avgPrice; }
    public void setAvgPrice(double avgPrice) { this.avgPrice = avgPrice; }
    
    public double getMinPrice() { return minPrice; }
    public void setMinPrice(double minPrice) { this.minPrice = minPrice; }
    
    public double getMaxPrice() { return maxPrice; }
    public void setMaxPrice(double maxPrice) { this.maxPrice = maxPrice; }
    
    public double getStdDev() { return stdDev; }
    public void setStdDev(double stdDev) { this.stdDev = stdDev; }
    
    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }
    
    public double getTotalVolume() { return totalVolume; }
    public void setTotalVolume(double totalVolume) { this.totalVolume = totalVolume; }
    
    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }
    
    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
    
    public AlertLevel getAlertLevel() { return alertLevel; }
    public void setAlertLevel(AlertLevel alertLevel) { this.alertLevel = alertLevel; }
    
    public String getAlertReason() { return alertReason; }
    public void setAlertReason(String alertReason) { this.alertReason = alertReason; }

    public boolean isAlert() {
        return alertLevel != AlertLevel.NORMAL;
    }

    @Override
    public String toString() {
        return String.format(
            "AGG | SRC:%s | SYMBOL:%s | AVG:%.2f | MIN:%.2f | MAX:%.2f | STD:%.2f | COUNT:%d | ALERT:%s | REASON:%s",
            source, symbol, avgPrice, minPrice, maxPrice, stdDev, count, alertLevel, alertReason
        );
    }
}
