package dev.resiliency.orders.order;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {
    @Id private UUID id;
    private String customerReference;
    private BigDecimal amount;
    private String currency;
    private String status;
    private int processingCount;
    private Instant createdAt;
    private Instant processedAt;

    protected Order() {}
    public Order(UUID id, String customerReference, BigDecimal amount, String currency) {
        this.id = id;
        this.customerReference = customerReference;
        this.amount = amount;
        this.currency = currency;
        this.status = "PENDING";
        this.createdAt = Instant.now();
    }
    public UUID getId() { return id; }
    public String getCustomerReference() { return customerReference; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public String getStatus() { return status; }
    public int getProcessingCount() { return processingCount; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getProcessedAt() { return processedAt; }
}
