package dev.resiliency.orders.demo;

import dev.resiliency.orders.order.OrderController.CreateOrder;
import dev.resiliency.orders.order.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@RestController
@RequestMapping("/demo")
@ConditionalOnProperty(name = "lab.demo-enabled", havingValue = "true")
public class DemoController {
    private final OrderService orders;
    private final JdbcTemplate jdbc;
    public DemoController(OrderService orders, JdbcTemplate jdbc) {
        this.orders = orders;
        this.jdbc = jdbc;
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.CREATED)
    @Transactional
    public OrderService.Created create(@Valid @RequestBody DemoOrder request) {
        var order = request.order();
        var created = orders.create(order.customerReference(), order.amount(), order.currency(), MDC.get("correlationId"));
        jdbc.update("INSERT INTO demo_failures(order_id, failures_remaining) VALUES (?, ?)", created.orderId(), request.failures());
        return created;
    }

    @PostMapping("/events/{eventId}/replay")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void replay(@PathVariable UUID eventId) {
        if (jdbc.update("UPDATE outbox_events SET published_at = NULL, next_attempt_at = now() WHERE event_id = ?", eventId) == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Event not found");
        }
    }

    @PutMapping("/orders/{orderId}/failures")
    public void failures(@PathVariable UUID orderId, @Valid @RequestBody FailurePlan plan) {
        if (jdbc.update("UPDATE demo_failures SET failures_remaining = ? WHERE order_id = ?", plan.failures(), orderId) == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Demo order not found");
        }
    }

    @GetMapping("/orders/{orderId}")
    public Map<String, Object> inspect(@PathVariable UUID orderId) {
        var rows = jdbc.queryForList("""
                SELECT o.id, o.status, o.processing_count, f.attempts, f.failures_remaining,
                       e.event_id, e.published_at, e.attempts AS publish_attempts,
                       (SELECT count(*) FROM processed_events p WHERE p.event_id = e.event_id) AS dedup_count,
                       (SELECT count(*) FROM dead_letters d WHERE d.record_key = o.id::text) AS dead_letter_count
                FROM orders o JOIN demo_failures f ON f.order_id = o.id JOIN outbox_events e ON e.order_id = o.id
                WHERE o.id = ?
                """, orderId);
        if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Demo order not found");
        return rows.getFirst();
    }

    @GetMapping("/dead-letters")
    public List<Map<String, Object>> deadLetters() {
        return jdbc.queryForList("SELECT * FROM dead_letters ORDER BY id DESC LIMIT 50");
    }

    public record DemoOrder(@NotNull @Valid CreateOrder order, @NotNull @Min(-1) @Max(100) Integer failures) {}
    public record FailurePlan(@NotNull @Min(-1) @Max(100) Integer failures) {}
}
