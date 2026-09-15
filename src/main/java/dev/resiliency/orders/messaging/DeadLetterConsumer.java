package dev.resiliency.orders.messaging;

import dev.resiliency.orders.observability.CorrelationFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;

@Component
public class DeadLetterConsumer {
    private final JdbcTemplate jdbc;
    public DeadLetterConsumer(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional
    @KafkaListener(id = "dead-letter-audit-v1", topics = OrderCreated.DLT, groupId = "dead-letter-audit-v1", concurrency = "1")
    public void receive(ConsumerRecord<String, String> record) {
        jdbc.update("""
                INSERT INTO dead_letters(topic, partition_id, record_offset, record_key, payload, correlation_id, exception_message)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
                """, record.topic(), record.partition(), record.offset(), record.key(),
                record.value() == null ? "null" : record.value(), header(record, CorrelationFilter.HEADER),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
    }

    private String header(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
