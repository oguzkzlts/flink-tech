package org.example.flink.models;

import java.io.Serializable;
import java.util.Objects;

public class PriceEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String eventId;
    private String symbol;
    private Double price;
    private Long timestamp;
    private String source;
    private Double volume;
    private Double high24h;
    private Double low24h;
    private Double priceChange24h;
    private String rawPayload;
    private ValidationStatus validationStatus;
    private Long processingTimestamp;

    public enum ValidationStatus {
        VALID, INVALID, SUSPICIOUS, ENRICHED
    }

    public PriceEvent() {
        this.processingTimestamp = System.currentTimeMillis();
        this.validationStatus = ValidationStatus.VALID;
    }

    public PriceEvent(String symbol, Double price, Long timestamp, String source) {
        this();
        this.symbol = symbol;
        this.price = price;
        this.timestamp = timestamp;
        this.source = source;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    
    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    
    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }
    
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    
    public Double getVolume() { return volume; }
    public void setVolume(Double volume) { this.volume = volume; }
    
    public Double getHigh24h() { return high24h; }
    public void setHigh24h(Double high24h) { this.high24h = high24h; }
    
    public Double getLow24h() { return low24h; }
    public void setLow24h(Double low24h) { this.low24h = low24h; }
    
    public Double getPriceChange24h() { return priceChange24h; }
    public void setPriceChange24h(Double priceChange24h) { this.priceChange24h = priceChange24h; }
    
    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
    
    public ValidationStatus getValidationStatus() { return validationStatus; }
    public void setValidationStatus(ValidationStatus validationStatus) { this.validationStatus = validationStatus; }
    
    public Long getProcessingTimestamp() { return processingTimestamp; }

    public String getCompositeKey() {
        return symbol + "|" + source;
    }

    public boolean isValid() {
        return validationStatus == ValidationStatus.VALID || validationStatus == ValidationStatus.ENRICHED;
    }

    public boolean isPriceWithinReasonableRange(double minPrice, double maxPrice) {
        return price != null && price >= minPrice && price <= maxPrice;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PriceEvent that = (PriceEvent) o;
        return Objects.equals(eventId, that.eventId) &&
               Objects.equals(symbol, that.symbol) &&
               Objects.equals(price, that.price) &&
               Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, symbol, price, timestamp, source);
    }

    @Override
    public String toString() {
        return String.format("SRC:%s | SYMBOL:%s | PRICE:%.2f | VOL:%.2f | TS:%d | STATUS:%s", 
                source, symbol, price, volume != null ? volume : 0.0, timestamp, validationStatus);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final PriceEvent event = new PriceEvent();

        public Builder symbol(String symbol) {
            event.setSymbol(symbol);
            return this;
        }

        public Builder price(Double price) {
            event.setPrice(price);
            return this;
        }

        public Builder timestamp(Long timestamp) {
            event.setTimestamp(timestamp);
            return this;
        }

        public Builder source(String source) {
            event.setSource(source);
            return this;
        }

        public Builder volume(Double volume) {
            event.setVolume(volume);
            return this;
        }

        public Builder eventId(String eventId) {
            event.setEventId(eventId);
            return this;
        }

        public Builder rawPayload(String rawPayload) {
            event.setRawPayload(rawPayload);
            return this;
        }

        public PriceEvent build() {
            return event;
        }
    }
}
