package dev.resiliency.orders.demo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Component
public class FailureSimulator {
    private final JdbcTemplate jdbc;
    private final boolean enabled;
    public FailureSimulator(JdbcTemplate jdbc, @Value("${lab.demo-enabled:false}") boolean enabled) {
        this.jdbc = jdbc;
        this.enabled = enabled;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean shouldFail(UUID orderId) {
        if (!enabled) return false;
        var remaining = jdbc.query("SELECT failures_remaining FROM demo_failures WHERE order_id = ? FOR UPDATE",
                (rs, n) -> rs.getInt(1), orderId);
        if (remaining.isEmpty()) return false;
        int failures = remaining.getFirst();
        jdbc.update("""
                UPDATE demo_failures SET attempts = attempts + 1,
                failures_remaining = CASE WHEN failures_remaining > 0 THEN failures_remaining - 1 ELSE failures_remaining END
                WHERE order_id = ?
                """, orderId);
        return failures != 0;
    }
}
