package dev.agenticintegrationpatterns.orderdesk.history;

import dev.agenticintegrationpatterns.orderdesk.OrderExceptionApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.Instant;
import java.util.Map;

import static dev.agenticintegrationpatterns.orderdesk.history.HistoryOperationalQueries.UNRESOLVED_RECOVERY;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class HistoryOperationalQueriesTest {
    private static final String TENANT = "tenant-ch19-query";
    private static final String RUN = "run-ch19-query";
    private static final String CASE = "case-ch19-query";
    private static final String SHA = "c".repeat(64);
    private static final Instant NOW = Instant.parse("2026-08-27T20:00:00Z");

    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        clearFixture();
        jdbc.update("""
                insert into investigation_run
                (tenant_id, run_id, case_id, correlation_id, command_id, plan_version,
                 state, completion_decision, deadline_at, attempt_count, version, fence_token,
                 created_at, updated_at)
                values (?,?,?,?,?,?,'COMPLETED','RECOVERY_REQUIRED',?,0,1,0,?,?)
                """, TENANT, RUN, CASE, "corr-ch19-query", "cmd-ch19-query", "plan-v1",
                NOW.plusSeconds(3_600), NOW, NOW);
    }

    @AfterEach
    void tearDown() {
        clearFixture();
    }

    @Test
    void unresolvedRecoveryQueryUsesActualStatesAndKeepsPlansWithoutEffectRows() {
        insertPlan("plan-observation", "OBSERVATION_REQUIRED");
        insertPlan("plan-decision", "RECOVERY_DECISION_REQUIRED");
        insertPlan("plan-compensation", "COMPENSATION_PENDING");
        insertPlan("plan-manual", "MANUAL_RECOVERY");
        insertPlan("plan-effect-wait", "FORWARD_RUNNING");
        insertEffect("plan-effect-wait", "effect-accepted", "ACCEPTED");
        insertPlan("plan-complete", "COMPLETED");

        var rows = new NamedParameterJdbcTemplate(jdbc).queryForList(
                UNRESOLVED_RECOVERY, Map.of("authorized_tenant", TENANT));

        assertThat(rows).extracting(row -> row.get("PLAN_ID"))
                .containsExactlyInAnyOrder("plan-observation", "plan-decision",
                        "plan-compensation", "plan-manual", "plan-effect-wait")
                .doesNotContain("plan-complete");
        assertThat(rows.stream().filter(row -> "plan-manual".equals(row.get("PLAN_ID")))
                .findFirst().orElseThrow().get("EFFECT_ID")).isNull();
    }

    private void insertPlan(String planId, String state) {
        jdbc.update("""
                insert into resolution_plan
                (tenant_id, plan_id, run_id, case_id, proposal_id, plan_state, recovery_owner,
                 deadline_at, evidence_set_ref, evidence_sha256, configuration_ref,
                 definition_sha256, next_event_number, last_event_id, version, created_at,
                 updated_at)
                values (?,?,?,?,?,?,?,?,'artifact://fixture/evidence',?,'config-ch19',?,1,?,1,?,?)
                """, TENANT, planId, RUN, CASE, "proposal-ch19", state,
                "team:recovery-operations", NOW.plusSeconds(1_800), SHA, SHA,
                "evt-" + planId, NOW, NOW);
    }

    private void insertEffect(String planId, String effectId, String observedState) {
        jdbc.update("""
                insert into effect_ledger
                (tenant_id, effect_id, run_id, case_id, decision_ref, policy_snapshot_ref,
                 effect_type, target_system, target_resource_key, warehouse_id, sku, quantity,
                 intent_sha256, target_contract_ref, target_idempotency_key, effect_state,
                 version, attempt_count, fence_token, created_at, updated_at)
                values (?,?,?,?,?,?,?,?,?,?,?,1,?,?,?, ?,1,0,0,?,?)
                """, TENANT, effectId, RUN, CASE, "decision-ch19", "policy-ch19",
                "TEST_EFFECT", "fixture-target", "resource-ch19", "warehouse-ch19", "sku-ch19",
                SHA, "target-contract-v1", "idem-" + effectId, observedState, NOW, NOW);
        jdbc.update("""
                insert into resolution_plan_effect
                (tenant_id, plan_id, step_number, effect_id, effect_role, effect_type,
                 target_contract_ref, authority_ref, authority_valid_until, evidence_sha256,
                 configuration_ref, reversibility, observed_state, observed_effect_version,
                 observation_evidence_ref)
                values (?,?,1,?,'FORWARD','TEST_EFFECT','target-contract-v1',?,?,?,
                        'config-ch19','REVERSIBLE',?,1,'fixture-observation')
                """, TENANT, planId, effectId, "authority-ch19", NOW.plusSeconds(600), SHA,
                observedState);
    }

    private void clearFixture() {
        jdbc.update("delete from resolution_plan_effect where tenant_id=?", TENANT);
        jdbc.update("delete from resolution_plan where tenant_id=?", TENANT);
        jdbc.update("delete from effect_ledger where tenant_id=?", TENANT);
        jdbc.update("delete from investigation_run where tenant_id=?", TENANT);
    }
}
