package dev.resiliency.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.resiliency.orders.messaging.*;
import dev.resiliency.orders.order.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@AutoConfigureObservability
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReliabilityIT {
    @Container static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.6");
    @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired OrderService orders;
    @Autowired OrderProcessor processor;
    @Autowired TransactionTemplate transaction;
    @Autowired KafkaTemplate<String, String> producer;
    @Autowired ObjectMapper json;
    @Autowired MeterRegistry metrics;
    @Autowired KafkaListenerEndpointRegistry listeners;

    private record Created(UUID orderId, UUID eventId, String correlationId) {}

    private Created create(int failures) {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Correlation-ID", "test-" + UUID.randomUUID());
        var response = http.postForEntity("/demo/orders", new HttpEntity<>(Map.of(
                "order", Map.of("customerReference", "integration", "amount", "12.50", "currency", "USD"),
                "failures", failures), headers), Created.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getHeaders().getFirst("X-Correlation-ID")).isEqualTo(response.getBody().correlationId());
        return response.getBody();
    }

    private long count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Long.class, args);
    }

    private void processed(Created created) {
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            assertThat(count("SELECT processing_count FROM orders WHERE id = ?", created.orderId())).isEqualTo(1);
            assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", created.eventId())).isEqualTo(1);
        });
    }

    private void replay(Created created) {
        assertThat(http.postForEntity("/demo/events/" + created.eventId() + "/replay", null, Void.class)
                .getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test @org.junit.jupiter.api.Order(1)
    void normalProcessingPublishesAnImmutableContractAndPropagatesCorrelation() throws Exception {
        var created = create(0);
        processed(created);
        var payload = jdbc.queryForObject("SELECT payload FROM outbox_events WHERE event_id = ?", String.class, created.eventId());
        var event = json.readValue(payload, OrderCreated.class);
        assertThat(event.orderId()).isEqualTo(created.orderId());
        assertThat(event.correlationId()).isEqualTo(created.correlationId());
        assertThat(event.schemaVersion()).isEqualTo(1);
        assertThat(event.payload().amount()).isEqualByComparingTo("12.50");
        assertThat(count("SELECT attempts FROM demo_failures WHERE order_id = ?", created.orderId())).isEqualTo(1);
        await().untilAsserted(() -> assertThat(count(
                "SELECT count(*) FROM outbox_events WHERE event_id = ? AND published_at IS NOT NULL", created.eventId())).isEqualTo(1));
    }

    @Test @org.junit.jupiter.api.Order(2)
    void orderAndOutboxRollbackTogether() {
        String customer = "rolled-back-" + UUID.randomUUID();
        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
            orders.create(customer, new BigDecimal("10.00"), "USD", "rollback-test");
            throw new IllegalStateException("Force rollback after outbox insert");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(count("SELECT count(*) FROM orders WHERE customer_reference = ?", customer)).isZero();
        assertThat(count("SELECT count(*) FROM outbox_events WHERE correlation_id = 'rollback-test'")).isZero();
    }

    @Test @org.junit.jupiter.api.Order(3)
    void retriesRollbackEffectsThenRecoverAfterTwoFailures() {
        var created = create(2);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(count("SELECT attempts FROM demo_failures WHERE order_id = ?", created.orderId())).isGreaterThanOrEqualTo(1);
            assertThat(count("SELECT processing_count FROM orders WHERE id = ?", created.orderId())).isZero();
            assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", created.eventId())).isZero();
        });
        processed(created);
        assertThat(count("SELECT attempts FROM demo_failures WHERE order_id = ?", created.orderId())).isEqualTo(3);
        assertThat(count("SELECT count(*) FROM dead_letters WHERE record_key = ?", created.orderId().toString())).isZero();
    }

    @Test @org.junit.jupiter.api.Order(4)
    void duplicateKafkaDeliveryHasNoSecondEffect() {
        var created = create(0);
        processed(created);
        double before = duplicateCount();
        replay(created);
        await().atMost(Duration.ofSeconds(20)).until(() -> duplicateCount() > before);
        assertThat(count("SELECT processing_count FROM orders WHERE id = ?", created.orderId())).isEqualTo(1);
        assertThat(count("SELECT attempts FROM demo_failures WHERE order_id = ?", created.orderId())).isEqualTo(1);
    }

    private double duplicateCount() {
        var counter = metrics.find("lab.consumer.duplicates").counter();
        return counter == null ? 0 : counter.count();
    }

    @Test @org.junit.jupiter.api.Order(5)
    void exhaustedRetriesReachDltWithOriginalPayloadAndCanBeReplayedAfterRepair() throws Exception {
        var created = create(-1);
        await().atMost(Duration.ofSeconds(35)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM dead_letters WHERE record_key = ?", created.orderId().toString())).isEqualTo(1));
        assertThat(count("SELECT attempts FROM demo_failures WHERE order_id = ?", created.orderId())).isEqualTo(4);
        assertThat(count("SELECT processing_count FROM orders WHERE id = ?", created.orderId())).isZero();
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", created.eventId())).isZero();
        var letter = jdbc.queryForMap("SELECT * FROM dead_letters WHERE record_key = ?", created.orderId().toString());
        assertThat(letter.get("correlation_id")).isEqualTo(created.correlationId());
        assertThat(letter.get("exception_message").toString()).contains("Simulated downstream failure");
        assertThat(json.readTree(letter.get("payload").toString()).get("eventId").asText()).isEqualTo(created.eventId().toString());

        http.put("/demo/orders/" + created.orderId() + "/failures", Map.of("failures", 0));
        replay(created);
        processed(created);
        assertThat(count("SELECT count(*) FROM dead_letters WHERE record_key = ?", created.orderId().toString())).isEqualTo(1);
    }

    @Test @org.junit.jupiter.api.Order(6)
    void malformedJsonGoesStraightToDltAndConsumerContinues() throws Exception {
        String key = UUID.randomUUID().toString();
        producer.send(OrderCreated.TOPIC, key, "{broken-json").get(15, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM dead_letters WHERE record_key = ? AND payload = '{broken-json'", key)).isEqualTo(1));
        processed(create(0));
    }

    @Test @org.junit.jupiter.api.Order(7)
    void concurrentDatabaseDeliveriesAreSerializedByUniqueDeduplicationKey() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO orders(id, customer_reference, amount, currency, status, created_at)
                VALUES (?, 'concurrent', 10, 'USD', 'PENDING', now())
                """, orderId);
        var event = new OrderCreated(eventId, "OrderCreated", 1, java.time.Instant.now(), "concurrent-test",
                orderId, new OrderCreated.Payload("concurrent", BigDecimal.TEN, "USD"));
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(6)) {
            var jobs = new ArrayList<Future<Boolean>>();
            for (int i = 0; i < 6; i++) jobs.add(executor.submit(() -> { start.await(); return processor.process(event); }));
            start.countDown();
            int effects = 0;
            for (var job : jobs) if (job.get(15, TimeUnit.SECONDS)) effects++;
            assertThat(effects).isEqualTo(1);
        }
        assertThat(count("SELECT processing_count FROM orders WHERE id = ?", orderId)).isEqualTo(1);
    }

    @Test @org.junit.jupiter.api.Order(8)
    void invalidRestInputDoesNotCreateAnOrder() {
        var response = http.postForEntity("/orders",
                Map.of("customerReference", "invalid-input", "amount", "-1", "currency", "usd"), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(count("SELECT count(*) FROM orders WHERE customer_reference = 'invalid-input'")).isZero();
    }

    @Test @org.junit.jupiter.api.Order(9)
    void healthAndConsumerMetricsAreExposed() {
        assertThat(http.getForEntity("/actuator/health/liveness", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(http.getForEntity("/actuator/health/readiness", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        var scrape = http.getForObject("/actuator/prometheus", String.class);
        assertThat(scrape).contains("kafka_consumer_", "spring_kafka_listener_seconds", "lab_consumer_processed_total");
        var container = listeners.getListenerContainer(OrderCreated.GROUP);
        assertThat(container).isNotNull();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(container.getAssignedPartitions()).hasSize(3));
    }

    @Test @org.junit.jupiter.api.Order(10)
    void aStoppedConsumerCatchesUpFromItsGroupOffsets() throws Exception {
        var listener = listeners.getListenerContainer(OrderCreated.GROUP);
        var stopped = new CountDownLatch(1);
        listener.stop(stopped::countDown);
        assertThat(stopped.await(20, TimeUnit.SECONDS)).isTrue();
        Created created;
        try {
            created = create(0);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(count(
                    "SELECT count(*) FROM outbox_events WHERE event_id = ? AND published_at IS NOT NULL", created.eventId())).isEqualTo(1));
            assertThat(count("SELECT processing_count FROM orders WHERE id = ?", created.orderId())).isZero();
        } finally {
            listener.start();
        }
        processed(created);
    }

    @Test @org.junit.jupiter.api.Order(11)
    void brokerOutageLeavesOutboxPendingAndRecoversWithoutLosingTheOrder() {
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(kafka.getContainerId()).exec();
        Created created;
        try {
            created = create(0);
            await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                    assertThat(count("SELECT attempts FROM outbox_events WHERE event_id = ?", created.eventId())).isGreaterThanOrEqualTo(1));
            assertThat(count("SELECT count(*) FROM outbox_events WHERE event_id = ? AND published_at IS NULL", created.eventId())).isEqualTo(1);
            assertThat(count("SELECT processing_count FROM orders WHERE id = ?", created.orderId())).isZero();
            assertThat(http.getForEntity("/actuator/health/readiness", String.class).getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(http.getForEntity("/actuator/health/liveness", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        } finally {
            docker.unpauseContainerCmd(kafka.getContainerId()).exec();
        }
        processed(created);
    }
}
