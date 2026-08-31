package dev.agenticintegrationpatterns.orderdesk.work;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.TypeConversionException;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.TypeConverterSupport;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkEnvelopeDecoderExceptionBoundaryTest {

    @Test
    void translatesMalformedJsonAtTheMessageBoundary() {
        var exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getMessage().setBody("{");

        assertThatThrownBy(() -> new WorkEnvelopeDecoder(new ObjectMapper()).process(exchange))
                .isInstanceOfSatisfying(InvalidWorkEnvelopeException.class,
                        failure -> {
                            assertThat(failure.violation())
                                    .isEqualTo(InvalidWorkEnvelopeException.Violation.MALFORMED);
                            assertThat(failure).hasCauseInstanceOf(tools.jackson.core.JacksonException.class);
                        });
    }

    @Test
    void translatesNullBodyAtTheMessageBoundary() {
        var exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getMessage().setBody(null);

        assertThatThrownBy(() -> new WorkEnvelopeDecoder(new ObjectMapper()).process(exchange))
                .isInstanceOfSatisfying(InvalidWorkEnvelopeException.class,
                        failure -> {
                            assertThat(failure.violation())
                                    .isEqualTo(InvalidWorkEnvelopeException.Violation.MALFORMED);
                            assertThat(failure).hasCauseInstanceOf(IllegalArgumentException.class)
                                    .hasRootCauseMessage("Message body is null");
                        });
    }

    @Test
    void translatesCamelTypeConversionFailureAtTheMessageBoundary() {
        var context = new DefaultCamelContext();
        context.getTypeConverterRegistry().addTypeConverter(
                String.class,
                UnconvertibleBody.class,
                new TypeConverterSupport() {
                    @Override
                    public <T> T convertTo(Class<T> type, Exchange exchange, Object value)
                            throws TypeConversionException {
                        throw new TypeConversionException(
                                value,
                                type,
                                new IllegalArgumentException("synthetic conversion failure"));
                    }
                });
        var exchange = new DefaultExchange(context);
        exchange.getMessage().setBody(new UnconvertibleBody());

        assertThatThrownBy(() -> new WorkEnvelopeDecoder(new ObjectMapper()).process(exchange))
                .isInstanceOfSatisfying(InvalidWorkEnvelopeException.class,
                        failure -> {
                            assertThat(failure.violation())
                                    .isEqualTo(InvalidWorkEnvelopeException.Violation.MALFORMED);
                            assertThat(failure).hasCauseInstanceOf(TypeConversionException.class);
                        });
    }

    @Test
    void doesNotMisclassifyProgrammingDefectsAsMalformedEnvelopes() {
        assertThatThrownBy(() -> new WorkEnvelopeDecoder(new ObjectMapper()).process(null))
                .isInstanceOf(NullPointerException.class);
    }

    private record UnconvertibleBody() {
    }
}
