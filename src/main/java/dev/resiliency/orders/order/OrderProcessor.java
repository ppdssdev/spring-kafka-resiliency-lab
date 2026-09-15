package dev.resiliency.orders.order;

import dev.resiliency.orders.demo.FailureSimulator;
import dev.resiliency.orders.messaging.OrderCreated;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderProcessor {
    private final JdbcTemplate jdbc;
    private final FailureSimulator failures;
    public OrderProcessor(JdbcTemplate jdbc, FailureSimulator failures) {
        this.jdbc = jdbc;
        this.failures = failures;
    }

    @Transactional
    public boolean process(OrderCreated event) {
        int inserted = jdbc.update("""
                INSERT INTO processed_events(consumer_group, event_id) VALUES (?, ?)
                ON CONFLICT DO NOTHING
                """, OrderCreated.GROUP, event.eventId());
        if (inserted == 0) return false;
        if (failures.shouldFail(event.orderId())) {
            throw new SimulatedProcessingException("Simulated downstream failure for order " + event.orderId());
        }
        // The business effect and deduplication marker commit together.
        int updated = jdbc.update("""
                UPDATE orders SET status = 'PROCESSED', processing_count = processing_count + 1, processed_at = now()
                WHERE id = ? AND status = 'PENDING'
                """, event.orderId());
        if (updated == 0) {
            throw new IllegalArgumentException("Order missing or already processed under another event ID");
        }
        return true;
    }

    public static class SimulatedProcessingException extends RuntimeException {
        public SimulatedProcessingException(String message) { super(message); }
    }
}
