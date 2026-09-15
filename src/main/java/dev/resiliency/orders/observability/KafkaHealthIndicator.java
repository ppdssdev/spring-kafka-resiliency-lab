package dev.resiliency.orders.observability;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.actuate.health.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator, AutoCloseable {
    private final AdminClient admin;
    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
    @Override
    public Health health() {
        try {
            var nodes = admin.describeCluster().nodes().get(3, TimeUnit.SECONDS);
            return nodes.isEmpty() ? Health.down().build() : Health.up().withDetail("brokers", nodes.size()).build();
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            return Health.down().withDetail("reason", "Kafka unavailable").build();
        }
    }
    @Override
    public void close() { admin.close(java.time.Duration.ofSeconds(3)); }
}
