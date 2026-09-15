package dev.resiliency.orders.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.resiliency.orders.messaging.OrderCreated;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {
    private final OrderRepository orders;
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public OrderService(OrderRepository orders, JdbcTemplate jdbc, ObjectMapper json) {
        this.orders = orders;
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional
    public Created create(String customerReference, BigDecimal amount, String currency, String correlationId) {
        var order = orders.saveAndFlush(new Order(UUID.randomUUID(), customerReference, amount, currency));
        var event = new OrderCreated(UUID.randomUUID(), "OrderCreated", 1, order.getCreatedAt(),
                correlationId, order.getId(), new OrderCreated.Payload(customerReference, amount, currency));
        event.validate();
        try {
            jdbc.update("INSERT INTO outbox_events(event_id, order_id, correlation_id, payload) VALUES (?, ?, ?, ?)",
                    event.eventId(), order.getId(), correlationId, json.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize order event", e);
        }
        return new Created(order.getId(), event.eventId(), correlationId);
    }

    public record Created(UUID orderId, UUID eventId, String correlationId) {}
}
