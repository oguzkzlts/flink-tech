package org.example.flink.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.OutputTag;
import org.example.flink.models.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseFunction extends RichMapFunction<String, PriceEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ParseFunction.class);
    
    private final OutputTag<String> deadLetterOutput;

    public ParseFunction(OutputTag<String> deadLetterOutput) {
        this.deadLetterOutput = deadLetterOutput;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void map(String line, org.apache.flink.util.Collector<PriceEvent> out) {
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

            out.collect(new PriceEvent(symbol, price, timestamp, source));
        } catch (Exception e) {
            // Route these to a "dead-letter" side output
            LOG.warn("Failed to parse line: {} - {}", line, e.getMessage());
            getRuntimeContext().output(deadLetterOutput, line);
        }
    }
}