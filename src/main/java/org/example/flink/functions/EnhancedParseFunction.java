package org.example.flink.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.example.flink.models.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnhancedParseFunction extends RichMapFunction<String, PriceEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedParseFunction.class);

    private transient Counter parseSuccessCounter;
    private transient Counter parseFailureCounter;
    private transient Counter emptyLineCounter;
    private transient Counter invalidFormatCounter;
    private transient Counter negativePriceCounter;
    private transient Counter numberFormatErrorCounter;
    private transient Counter unexpectedErrorCounter;
    private transient Meter parseLatencyMeter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parseSuccessCounter = getRuntimeContext().getMetricGroup().counter("parse.success");
        parseFailureCounter = getRuntimeContext().getMetricGroup().counter("parse.failure");
        emptyLineCounter = getRuntimeContext().getMetricGroup().counter("parse.empty.line");
        invalidFormatCounter = getRuntimeContext().getMetricGroup().counter("parse.invalid.format");
        negativePriceCounter = getRuntimeContext().getMetricGroup().counter("parse.negative.price");
        numberFormatErrorCounter = getRuntimeContext().getMetricGroup().counter("parse.number.format.error");
        unexpectedErrorCounter = getRuntimeContext().getMetricGroup().counter("parse.unexpected.error");
        parseLatencyMeter = getRuntimeContext().getMetricGroup().meter("parse.latency", new MeterView(60));
    }

    @Override
    public PriceEvent map(String line) throws Exception {
        long startTime = System.nanoTime();
        
        if (line == null || line.trim().isEmpty()) {
            if (emptyLineCounter != null) emptyLineCounter.inc();
            LOG.warn("Received empty or null line");
            return null;
        }

        try {
            String[] parts = line.split(":", 2);
            if (parts.length < 2) {
                if (invalidFormatCounter != null) invalidFormatCounter.inc();
                LOG.warn("Invalid format, missing source prefix: {}", line);
                return null;
            }

            String source = parts[0].trim().toUpperCase();
            String[] values = parts[1].split(",");
            
            if (values.length < 2) {
                if (invalidFormatCounter != null) invalidFormatCounter.inc();
                LOG.warn("Invalid format, missing price: {}", line);
                return null;
            }

            String symbol = values[0].trim().toUpperCase();
            if (symbol.isEmpty()) {
                if (invalidFormatCounter != null) invalidFormatCounter.inc();
                LOG.warn("Empty symbol: {}", line);
                return null;
            }

            double price = Double.parseDouble(values[1].trim());
            if (price < 0) {
                if (negativePriceCounter != null) negativePriceCounter.inc();
                LOG.warn("Negative price detected: {} for {}", price, symbol);
                return null;
            }

            long timestamp = (values.length > 2) 
                    ? Long.parseLong(values[2].trim()) 
                    : System.currentTimeMillis();

            double volume = (values.length > 3) 
                    ? Double.parseDouble(values[3].trim()) 
                    : 0.0;

            PriceEvent event = PriceEvent.builder()
                    .eventId(java.util.UUID.randomUUID().toString())
                    .symbol(symbol)
                    .price(price)
                    .timestamp(timestamp)
                    .source(source)
                    .volume(volume)
                    .rawPayload(line)
                    .build();

            if (parseSuccessCounter != null) parseSuccessCounter.inc();
            LOG.debug("Successfully parsed: {}", event);
            
            long latency = System.nanoTime() - startTime;
            if (parseLatencyMeter != null) parseLatencyMeter.markEvent((long)(latency / 1000000));
            
            return event;

        } catch (NumberFormatException e) {
            if (numberFormatErrorCounter != null) numberFormatErrorCounter.inc();
            LOG.error("Number format error parsing: {} - {}", line, e.getMessage());
            return null;
        } catch (Exception e) {
            if (unexpectedErrorCounter != null) unexpectedErrorCounter.inc();
            LOG.error("Unexpected error parsing: {} - {}", line, e.getMessage(), e);
            return null;
        }
    }
}
