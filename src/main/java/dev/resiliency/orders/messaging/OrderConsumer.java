package dev.resiliency.orders.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.resiliency.orders.order.OrderProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);
    private final ObjectMapper json;
    private final OrderProcessor processor;
    private final MeterRegistry metrics;
    public OrderConsumer(ObjectMapper json, OrderProcessor processor, MeterRegistry metrics) {
        this.json = json;
        this.processor = processor;
        this.metrics = metrics;
    }

    @KafkaListener(id = OrderCreated.GROUP, topics = OrderCreated.TOPIC, groupId = OrderCreated.GROUP)
    public void receive(ConsumerRecord<String, String> record) {
        OrderCreated event;
        try {
            event = json.readValue(record.value(), OrderCreated.class);
            if (event == null) throw new IllegalArgumentException("Event cannot be null");
            event.validate();
            if (!event.orderId().toString().equals(record.key())) {
                throw new IllegalArgumentException("Kafka key must equal orderId");
            }
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid event JSON", e);
        }
        try (var ignored = MDC.putCloseable("correlationId", event.correlationId());
             var eventContext = MDC.putCloseable("eventId", event.eventId().toString());
             var orderContext = MDC.putCloseable("orderId", event.orderId().toString())) {
            boolean processed = processor.process(event);
            metrics.counter(processed ? "lab.consumer.processed" : "lab.consumer.duplicates").increment();
            log.info(processed ? "Order processed and transaction committed" : "Duplicate event ignored");
        }
    }
}
