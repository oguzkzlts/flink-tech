package org.example.flink.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.models.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseFunction extends ProcessFunction<String, PriceEvent> {
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
    public void processElement(String line, Context ctx, Collector<PriceEvent> out) {
        try {
            String[] parts = line.split(":");
            String source = parts[0];

            String[] values = parts[1].split(",");
            String symbol = values[0].trim().toUpperCase();
            double price = Double.parseDouble(values[1].trim());

            long timestamp = (values.length > 2)
                    ? Long.parseLong(values[2].trim())
                    : System.currentTimeMillis();

            out.collect(new PriceEvent(symbol, price, timestamp, source));
        } catch (Exception e) {
            LOG.warn("Failed to parse line: {} - {}", line, e.getMessage());
            ctx.output(deadLetterOutput, line);
        }
    }
}