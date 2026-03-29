package org.example.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.flink.models.PriceEvent;

public class ParseFunction implements MapFunction<String, PriceEvent> {
    @Override
    public PriceEvent map(String line) {
        try {
            // Expected format: "KAFKA:BTC,55000" or "SOCKET:ETH,2000,1672531200000"
            String[] parts = line.split(":");
            String source = parts[0]; // KAFKA or SOCKET

            String[] values = parts[1].split(",");
            String symbol = values[0].trim().toUpperCase();
            double price = Double.parseDouble(values[1].trim());

            // Logic: If the input has a 3rd column, use it as the event timestamp.
            // Otherwise, use the current system time as a fallback.
            long timestamp = (values.length > 2)
                    ? Long.parseLong(values[2].trim())
                    : System.currentTimeMillis();

            return new PriceEvent(symbol, price, timestamp, source);
        } catch (Exception e) {
            //TODO: route these to a "dead-letter" side output
            return null;
        }
    }
}