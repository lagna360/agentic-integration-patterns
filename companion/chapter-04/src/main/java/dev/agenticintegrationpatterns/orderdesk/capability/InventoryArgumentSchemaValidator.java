package dev.agenticintegrationpatterns.orderdesk.capability;

import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public final class InventoryArgumentSchemaValidator {
    public static final String SCHEMA_ID =
            "urn:agentic-integration-patterns:inventory-availability-read:1";

    private final Schema schema;

    public InventoryArgumentSchemaValidator(ObjectMapper mapper) {
        try (var input = getClass().getResourceAsStream(
                "/contracts/inventory-availability-read-v1.schema.json")) {
            if (input == null) {
                throw new IllegalStateException("Inventory argument schema is missing");
            }
            JsonNode schemaNode = mapper.readTree(input);
            this.schema = SchemaRegistry
                    .withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                    .getSchema(schemaNode);
        } catch (Exception failure) {
            throw new IllegalStateException("Inventory argument schema cannot be loaded", failure);
        }
    }

    public void validate(JsonNode arguments) {
        if (arguments == null || !schema.validate(arguments).isEmpty()) {
            throw new CapabilityGatewayException(
                    CapabilityGatewayException.Reason.ARGUMENT_SCHEMA_VIOLATION,
                    "Capability arguments do not satisfy the registered schema");
        }
    }
}
