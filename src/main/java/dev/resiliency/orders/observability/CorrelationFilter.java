package dev.resiliency.orders.observability;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.IOException;
import java.util.UUID;

@Component
public class CorrelationFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Correlation-ID";
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var value = request.getHeader(HEADER);
        var correlationId = value != null && value.matches("[A-Za-z0-9._-]{1,100}")
                ? value : UUID.randomUUID().toString();
        response.setHeader(HEADER, correlationId);
        try (var ignored = MDC.putCloseable("correlationId", correlationId)) {
            chain.doFilter(request, response);
        }
    }
}
