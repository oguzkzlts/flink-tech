package org.example.flink.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.example.flink.models.DeadLetterEvent;
import org.example.flink.models.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterQueueHandler extends RichMapFunction<String, DeadLetterEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterQueueHandler.class);

    private transient Counter deadLetterCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        deadLetterCounter = getRuntimeContext().getMetricGroup().counter("dead.letters.created");
    }

    @Override
    public DeadLetterEvent map(String failedPayload) throws Exception {
        DeadLetterEvent dlqEvent = new DeadLetterEvent();
        dlqEvent.setOriginalPayload(failedPayload);
        dlqEvent.setErrorMessage("Failed to parse after retries");
        dlqEvent.setErrorType("PARSE_FAILURE");
        dlqEvent.setSource("unknown");
        dlqEvent.setRetryCount(0);
        
        deadLetterCounter.inc();
        LOG.error("Dead letter created for payload: {}", failedPayload);
        
        return dlqEvent;
    }
}
