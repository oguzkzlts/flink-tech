package org.example.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.flink.models.PriceEvent;

public class ParseFunction implements MapFunction<String, PriceEvent> {
    @Override
    public PriceEvent map(String line) {
        try {
            String[] parts = line.split(":");
            String source = parts[0];
            String[] values = parts[1].split(",");
            String symbol = values[0].trim().toUpperCase();
            double price = Double.parseDouble(values[1].trim());
            return new PriceEvent(symbol, source, price);
        } catch (Exception e) {
            return null;
        }
    }
}
