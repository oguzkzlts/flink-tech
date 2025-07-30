package org.example.flink.utils;

public class ThresholdUtil {

    public static double getThreshold(String symbol) {
        switch (symbol.toUpperCase()) {
            case "BTC":
                return 50000.0;
            case "ETH":
                return 2000.0;
            case "DOGE":
                return 0.1;
            default:
                return Double.MAX_VALUE;
        }
    }
}
