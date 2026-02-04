package com.slf4u0.eventscollectorservice.metrics;

import com.slf4u0.eventscollectorservice.repository.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class AppMetrics {

    private final Counter eventsProcessedTotal;
    private final Counter eventsDuplicatesTotal;
    private final Counter outboxPublishSuccessTotal;
    private final Counter outboxPublishFailTotal;

    public AppMetrics(MeterRegistry registry) {
        this.eventsProcessedTotal = Counter.builder("events.processed.total")
                .description("Total number of processed events")
                .register(registry);

        this.eventsDuplicatesTotal = Counter.builder("events.duplicates.total")
                .description("Total number of duplicate events (handled in outbox logic)")
                .register(registry);

        this.outboxPublishSuccessTotal = Counter.builder("outbox.publish.success.total")
                .description("Total number of successful outbox publishes")
                .register(registry);

        this.outboxPublishFailTotal = Counter.builder("outbox.publish.fail.total")
                .description("Total number of failed outbox publishes")
                .register(registry);
    }

    public void incrementEventsProcessed() {
        eventsProcessedTotal.increment();
    }

    public void incrementEventsDuplicates() {
        eventsDuplicatesTotal.increment();
    }

    public void incrementOutboxPublishSuccess() {
        outboxPublishSuccessTotal.increment();
    }

    public void incrementOutboxFail() {
        outboxPublishFailTotal.increment();
    }

    public void registerOutboxPendingGauge(MeterRegistry registry, OutboxRepository outboxRepository) {
        Gauge.builder("outbox.pending.count", outboxRepository::countPending)
                .description("Current count of pending records in the outbox")
                .register(registry);
    }

}
