package org.example.flink.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.flink.models.PriceEvent;

import static org.example.flink.utils.ThresholdUtil.getThreshold;

public class AggregationFunction implements WindowFunction<PriceEvent, String, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<PriceEvent> input, Collector<String> out) {
        int count = 0;
        double sum = 0;

        for (PriceEvent event : input) {
            sum += event.getPrice();
            count++;
        }

        double avg = sum / count;
        String[] keyParts = key.split("\\|");
        String symbol = keyParts[0];
        String source = keyParts[1];
        String result = String.format("SRC:%s | SYMBOL:%s | AVG_PRICE:%.2f | COUNT:%d", source, symbol, avg, count);

        if (avg > getThreshold(symbol)) {
            out.collect("ALERT: " + result);
        } else {
            out.collect("NORMAL: " + result);
        }
    }
}
