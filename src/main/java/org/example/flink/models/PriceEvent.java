package org.example.flink.models;

public class PriceEvent {
    public String symbol;
    public String source;

    public double price;

    public PriceEvent() {}

    public PriceEvent(String symbol, String source, double price) {
        this.symbol = symbol;
        this.source = source;
        this.price = price;
    }

    public double getPrice() {
        return price;
    }

    public String getSource() {
        return source;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getCompositeKey() {
        return symbol + "|" + source;
    }

    @Override
    public String toString() {
        return String.format("SRC:%s | SYMBOL:%s | PRICE:%.2f", source, symbol, price);
    }
}

