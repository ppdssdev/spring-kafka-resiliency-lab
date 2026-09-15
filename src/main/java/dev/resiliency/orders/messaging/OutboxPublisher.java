package dev.resiliency.orders.messaging;

import dev.resiliency.orders.observability.CorrelationFilter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate transactions;
    private final MeterRegistry metrics;

    public OutboxPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka,
            TransactionTemplate transactions, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.transactions = transactions;
        this.metrics = metrics;
        metrics.gauge("lab.outbox.pending", jdbc, db ->
                db.queryForObject("SELECT count(*) FROM outbox_events WHERE published_at IS NULL", Long.class));
    }

    @Scheduled(fixedDelayString = "${lab.outbox.poll-ms}")
    public void poll() {
        try {
            for (int i = 0; i < 20 && Boolean.TRUE.equals(transactions.execute(status -> publishOne())); i++) {
                // A separate short transaction per event bounds lock duration and permits other publishers.
            }
        } catch (Exception e) {
            metrics.counter("lab.outbox.poll.failures").increment();
            log.warn("Outbox poll failed; pending records will be retried", e);
        }
    }

    private boolean publishOne() {
        var rows = jdbc.query("""
                SELECT event_id, order_id, correlation_id, payload, attempts FROM outbox_events
                WHERE published_at IS NULL AND next_attempt_at <= now()
                ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
                """, (rs, n) -> new Pending(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
                rs.getString(3), rs.getString(4), rs.getInt(5)));
        if (rows.isEmpty()) return false;
        var row = rows.getFirst();
        try (var ignored = MDC.putCloseable("correlationId", row.correlationId());
             var eventContext = MDC.putCloseable("eventId", row.eventId().toString())) {
            try {
                var record = new ProducerRecord<String, String>(OrderCreated.TOPIC, row.orderId().toString(), row.payload());
                record.headers().add(CorrelationFilter.HEADER, row.correlationId().getBytes(StandardCharsets.UTF_8));
                kafka.send(record).get(12, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                long delaySeconds = Math.min(60, 1L << Math.min(row.attempts(), 6));
                jdbc.update("""
                        UPDATE outbox_events SET attempts = attempts + 1,
                        next_attempt_at = now() + (? * interval '1 second') WHERE event_id = ?
                        """, delaySeconds, row.eventId());
                metrics.counter("lab.outbox.publish.failures").increment();
                log.warn("Kafka publication failed; outbox retry in {} seconds", delaySeconds, e);
                return false;
            }
            // Broker acknowledgement precedes this update. A crash here can republish the same event ID.
            jdbc.update("UPDATE outbox_events SET published_at = now(), attempts = attempts + 1 WHERE event_id = ?", row.eventId());
            metrics.counter("lab.outbox.published").increment();
            log.info("Outbox event acknowledged by Kafka");
        }
        return true;
    }

    private record Pending(UUID eventId, UUID orderId, String correlationId, String payload, int attempts) {}
}
