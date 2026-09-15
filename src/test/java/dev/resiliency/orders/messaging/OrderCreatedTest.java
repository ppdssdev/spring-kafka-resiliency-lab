package dev.resiliency.orders.messaging;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

class OrderCreatedTest {
    @Test
    void rejectsUnsupportedVersionAndUnsafeCorrelation() {
        var payload = new OrderCreated.Payload("customer", new BigDecimal("12.50"), "USD");
        assertThatThrownBy(() -> new OrderCreated(UUID.randomUUID(), "OrderCreated", 2,
                Instant.now(), "correlation", UUID.randomUUID(), payload).validate())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderCreated(UUID.randomUUID(), "OrderCreated", 1,
                Instant.now(), "unsafe\ncorrelation", UUID.randomUUID(), payload).validate())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
