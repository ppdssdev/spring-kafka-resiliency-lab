package dev.resiliency.orders.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.slf4j.MDC;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderService service;
    private final OrderRepository orders;
    public OrderController(OrderService service, OrderRepository orders) {
        this.service = service;
        this.orders = orders;
    }

    @PostMapping
    public ResponseEntity<OrderService.Created> create(@Valid @RequestBody CreateOrder request) {
        var result = service.create(request.customerReference(), request.amount(), request.currency(), MDC.get("correlationId"));
        return ResponseEntity.created(URI.create("/orders/" + result.orderId())).body(result);
    }

    @GetMapping("/{id}")
    public Order get(@PathVariable UUID id) {
        return orders.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found"));
    }

    public record CreateOrder(@NotBlank @Size(max = 100) String customerReference,
            @NotNull @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal amount,
            @NotNull @Pattern(regexp = "[A-Z]{3}") String currency) {}
}
