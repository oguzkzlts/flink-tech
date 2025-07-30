package org.example.flink.functions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class AlertRouter extends ProcessFunction<String, String> {

    private final OutputTag<String> alertOutput;
    private final OutputTag<String> normalOutput;

    public AlertRouter(OutputTag<String> alertOutput, OutputTag<String> normalOutput) {
        this.alertOutput = alertOutput;
        this.normalOutput = normalOutput;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
        if (value.startsWith("ALERT:")) {
            ctx.output(alertOutput, value);
        } else {
            ctx.output(normalOutput, value);
        }
    }
}
