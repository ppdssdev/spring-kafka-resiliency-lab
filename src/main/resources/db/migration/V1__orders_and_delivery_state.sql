CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_reference VARCHAR(100) NOT NULL,
    amount NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'PROCESSED')),
    processing_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ
);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    correlation_id VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending_idx ON outbox_events (next_attempt_at, created_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    consumer_group VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE dead_letters (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic VARCHAR(200) NOT NULL,
    partition_id INTEGER NOT NULL,
    record_offset BIGINT NOT NULL,
    record_key TEXT,
    payload TEXT NOT NULL,
    correlation_id TEXT,
    exception_message TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, partition_id, record_offset)
);

-- Lab-only controls. Attempts must survive the business transaction's rollback.
CREATE TABLE demo_failures (
    order_id UUID PRIMARY KEY REFERENCES orders(id),
    failures_remaining INTEGER NOT NULL CHECK (failures_remaining >= -1),
    attempts INTEGER NOT NULL DEFAULT 0
);
