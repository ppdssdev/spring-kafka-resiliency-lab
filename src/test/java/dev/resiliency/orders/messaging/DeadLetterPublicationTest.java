package dev.resiliency.orders.messaging;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DeadLetterPublicationTest {
    @Test
    @SuppressWarnings("unchecked")
    void failedDeadLetterSendDoesNotRecoverSourceRecord() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        ProducerFactory<String, String> factory = mock(ProducerFactory.class);
        when(kafka.getProducerFactory()).thenReturn(factory);
        when(factory.getConfigurationProperties()).thenReturn(Map.of("delivery.timeout.ms", 1000));
        when(kafka.partitionsFor(OrderCreated.DLT)).thenReturn(List.of(
                new PartitionInfo(OrderCreated.DLT, 0, null, null, null)));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("DLT broker unavailable")));
        var registry = new SimpleMeterRegistry();
        var handler = new KafkaConfiguration().kafkaErrorHandler(kafka, registry);
        var record = new ConsumerRecord<>(OrderCreated.TOPIC, 0, 42L, "key", "{invalid");
        var exception = new IllegalArgumentException("Invalid JSON");
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);

        assertThat(handler.handleOne(exception, record, consumer, container)).isFalse();
        assertThat(registry.find("lab.consumer.dead.lettered").counter()).isNull();

        SendResult<String, String> result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(mock(RecordMetadata.class));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(result));
        assertThat(handler.handleOne(exception, record, consumer, container)).isTrue();
        assertThat(registry.get("lab.consumer.dead.lettered").counter().count()).isEqualTo(1);
        verify(kafka, times(2)).send(any(ProducerRecord.class));
    }
}
