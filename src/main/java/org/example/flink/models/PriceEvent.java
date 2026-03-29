package org.example.flink.models;

import java.io.Serializable;

public class PriceEvent implements Serializable {
    private String symbol;
    private Double price;
    private Long timestamp;
    private String source;

    public PriceEvent() {}

    public PriceEvent(String symbol, Double price, Long timestamp, String source) {
        this.symbol = symbol;
        this.price = price;
        this.timestamp = timestamp;
        this.source = source;
    }

    public String getSymbol() { return symbol; }
    public Double getPrice() { return price; }
    public Long getTimestamp() { return timestamp; }
    public String getSource() { return source; }

    public String getCompositeKey() {
        return symbol + "|" + source;
    }

    @Override
    public String toString() {
        return String.format("SRC:%s | SYMBOL:%s | PRICE:%.2f | TS:%d", source, symbol, price, timestamp);
    }
}
