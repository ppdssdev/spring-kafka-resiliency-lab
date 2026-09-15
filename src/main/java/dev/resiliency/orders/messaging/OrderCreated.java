package dev.resiliency.orders.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreated(UUID eventId, String eventType, int schemaVersion,
        Instant occurredAt, String correlationId, UUID orderId, Payload payload) {
    public static final String TOPIC = "orders.created.v1";
    public static final String DLT = TOPIC + ".DLT";
    public static final String GROUP = "order-processor-v1";
    public record Payload(String customerReference, BigDecimal amount, String currency) {}

    public void validate() {
        if (eventId == null || orderId == null || occurredAt == null
                || !"OrderCreated".equals(eventType) || schemaVersion != 1
                || correlationId == null || !correlationId.matches("[A-Za-z0-9._-]{1,100}")
                || payload == null || payload.customerReference() == null
                || payload.customerReference().isBlank() || payload.customerReference().length() > 100
                || payload.amount() == null || payload.amount().signum() <= 0
                || payload.amount().scale() > 2 || payload.amount().precision() - payload.amount().scale() > 10
                || payload.currency() == null || !payload.currency().matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("Invalid OrderCreated v1 contract");
        }
    }
}
