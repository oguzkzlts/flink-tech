package org.example.flink.functions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.models.AggregatedResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnhancedAlertRouter extends ProcessFunction<AggregatedResult, AggregatedResult> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedAlertRouter.class);

    private final OutputTag<AggregatedResult> alertOutput;
    private final OutputTag<AggregatedResult> normalOutput;
    private final OutputTag<AggregatedResult> anomalyOutput;

    public EnhancedAlertRouter(OutputTag<AggregatedResult> alertOutput, 
                               OutputTag<AggregatedResult> normalOutput,
                               OutputTag<AggregatedResult> anomalyOutput) {
        this.alertOutput = alertOutput;
        this.normalOutput = normalOutput;
        this.anomalyOutput = anomalyOutput;
    }

    @Override
    public void processElement(AggregatedResult value, Context ctx, Collector<AggregatedResult> out) throws Exception {
        switch (value.getAlertLevel()) {
            case CRITICAL:
                LOG.warn("CRITICAL ALERT: {}", value);
                ctx.output(alertOutput, value);
                break;
            case WARNING:
                LOG.info("WARNING ALERT: {}", value);
                ctx.output(alertOutput, value);
                break;
            case ANOMALY:
                LOG.warn("ANOMALY DETECTED: {}", value);
                ctx.output(anomalyOutput, value);
                break;
            case NORMAL:
            default:
                ctx.output(normalOutput, value);
                break;
        }
    }
}
