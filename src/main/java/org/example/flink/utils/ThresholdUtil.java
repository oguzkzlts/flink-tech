package org.example.flink.utils;

public class ThresholdUtil {
    public static double getThreshold(String symbol) {
        return ThresholdServiceClient.getThreshold(symbol);
    }
}
