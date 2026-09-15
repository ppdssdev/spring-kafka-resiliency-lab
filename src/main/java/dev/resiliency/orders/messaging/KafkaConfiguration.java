package dev.resiliency.orders.messaging;

import dev.resiliency.orders.observability.CorrelationFilter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.*;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;
import java.time.Duration;
import java.nio.charset.StandardCharsets;

@Configuration
public class KafkaConfiguration {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);
    @Bean NewTopic ordersTopic() {
        return TopicBuilder.name(OrderCreated.TOPIC).partitions(3).replicas(1).build();
    }
    @Bean NewTopic deadLetterTopic() {
        return TopicBuilder.name(OrderCreated.DLT).partitions(3).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka, MeterRegistry metrics) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(OrderCreated.DLT, record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(15));
        var backoff = new ExponentialBackOffWithMaxRetries(3);
        backoff.setInitialInterval(1000);
        backoff.setMultiplier(2);
        backoff.setMaxInterval(4000);
        var handler = new DefaultErrorHandler((record, exception) -> {
            recoverer.accept(record, exception);
            metrics.counter("lab.consumer.dead.lettered").increment();
            log.warn("Dead-letter publication acknowledged for {}-{}@{}", record.topic(), record.partition(), record.offset());
        }, backoff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setResetStateOnExceptionChange(false);
        // An unavailable DLT audit database must never create a recursive DLT loop or discard a record.
        handler.setBackOffFunction((record, exception) -> OrderCreated.DLT.equals(record.topic())
                ? new FixedBackOff(2000, FixedBackOff.UNLIMITED_ATTEMPTS) : backoff);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception exception, int deliveryAttempt) {
                metrics.counter("lab.consumer.failures").increment();
                if (deliveryAttempt > 1) metrics.counter("lab.consumer.retries").increment();
                var header = record.headers().lastHeader(CorrelationFilter.HEADER);
                try (var ignored = MDC.putCloseable("correlationId",
                        header == null ? "unknown" : new String(header.value(), StandardCharsets.UTF_8))) {
                    log.warn("Consumer failed topic={} partition={} offset={} attempt={}: {}",
                            record.topic(), record.partition(), record.offset(), deliveryAttempt, exception.getMessage());
                }
            }
        });
        return handler;
    }
}
