package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.AssessmentOutcome;
import dev.agenticintegrationpatterns.orderdesk.model.AuditRecord;
import dev.agenticintegrationpatterns.orderdesk.model.ProcessingFailure;
import dev.agenticintegrationpatterns.orderdesk.model.ResolutionProposed;
import dev.agenticintegrationpatterns.orderdesk.model.ManualReviewRequired;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class InMemoryAuditRecorder implements Processor {
    private final List<AuditRecord> records = new CopyOnWriteArrayList<>();

    @Override
    public void process(Exchange exchange) {
        Object body = exchange.getMessage().getBody();
        if (body instanceof AssessmentOutcome outcome) {
            String provider = outcome instanceof ResolutionProposed proposal ? proposal.provider()
                    : ((ManualReviewRequired) outcome).provider();
            String model = outcome instanceof ResolutionProposed proposal ? proposal.model()
                    : ((ManualReviewRequired) outcome).model();
            String instructionVersion = outcome instanceof ResolutionProposed proposal
                    ? proposal.instructionVersion()
                    : ((ManualReviewRequired) outcome).instructionVersion();
            records.add(new AuditRecord(Instant.now(), outcome.correlationId(), outcome.tenantId(), outcome.caseId(),
                    body.getClass().getSimpleName(), provider, model, instructionVersion));
        } else if (body instanceof ProcessingFailure failure) {
            records.add(new AuditRecord(Instant.now(), failure.correlationId(), failure.tenantId(), failure.caseId(),
                    failure.kind().name(), null, null, null));
        }
    }

    public List<AuditRecord> records() {
        return List.copyOf(records);
    }

    public void reset() {
        records.clear();
    }
}
