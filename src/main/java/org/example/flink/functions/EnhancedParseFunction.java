package org.example.flink.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.example.flink.config.FlinkJobConfig;
import org.example.flink.models.DeadLetterEvent;
import org.example.flink.models.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EnhancedParseFunction extends RichMapFunction<String, PriceEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedParseFunction.class);

    private transient Counter parseSuccessCounter;
    private transient Counter parseFailureCounter;
    private transient FlinkJobConfig config;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parseSuccessCounter = getRuntimeContext().getMetricGroup().counter("parse.success");
        parseFailureCounter = getRuntimeContext().getMetricGroup().counter("parse.failure");
        
        Object configObj = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap().get("job.config");
        if (configObj instanceof FlinkJobConfig) {
            this.config = (FlinkJobConfig) configObj;
        } else {
            this.config = new FlinkJobConfig();
        }
    }

    @Override
    public PriceEvent map(String line) throws Exception {
        if (line == null || line.trim().isEmpty()) {
            if (parseFailureCounter != null) parseFailureCounter.inc();
            LOG.warn("Received empty or null line");
            return null;
        }

        try {
            String[] parts = line.split(":", 2);
            if (parts.length < 2) {
                if (parseFailureCounter != null) parseFailureCounter.inc();
                LOG.warn("Invalid format, missing source prefix: {}", line);
                return null;
            }

            String source = parts[0].trim().toUpperCase();
            String[] values = parts[1].split(",");
            
            if (values.length < 2) {
                if (parseFailureCounter != null) parseFailureCounter.inc();
                LOG.warn("Invalid format, missing price: {}", line);
                return null;
            }

            String symbol = values[0].trim().toUpperCase();
            if (symbol.isEmpty()) {
                if (parseFailureCounter != null) parseFailureCounter.inc();
                LOG.warn("Empty symbol: {}", line);
                return null;
            }

            double price = Double.parseDouble(values[1].trim());
            if (price < 0) {
                if (parseFailureCounter != null) parseFailureCounter.inc();
                LOG.warn("Negative price detected: {} for {}", price, symbol);
                return null;
            }

            if (config != null && (price < config.getMinPriceThreshold() || price > config.getMaxPriceThreshold())) {
                LOG.warn("Price outside reasonable range: {} for {}", price, symbol);
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
            return event;

        } catch (NumberFormatException e) {
            if (parseFailureCounter != null) parseFailureCounter.inc();
            LOG.error("Number format error parsing: {} - {}", line, e.getMessage());
            return null;
        } catch (Exception e) {
            if (parseFailureCounter != null) parseFailureCounter.inc();
            LOG.error("Unexpected error parsing: {} - {}", line, e.getMessage(), e);
            return null;
        }
    }
}
