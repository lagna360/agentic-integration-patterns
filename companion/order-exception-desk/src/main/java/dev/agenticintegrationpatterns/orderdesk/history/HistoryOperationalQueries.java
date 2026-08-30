package dev.agenticintegrationpatterns.orderdesk.history;

/** Executed teaching queries over the existing durable Chapter 16 resolution records. */
public final class HistoryOperationalQueries {
    private HistoryOperationalQueries() {}

    public static final String UNRESOLVED_RECOVERY = """
            -- tag::unresolved-recovery-query[]
            select p.tenant_id,
                   p.plan_id,
                   p.plan_state,
                   p.recovery_owner,
                   e.effect_id,
                   e.observed_state,
                   p.deadline_at,
                   p.updated_at
            from resolution_plan p
            left join resolution_plan_effect e
              on e.tenant_id = p.tenant_id
             and e.plan_id = p.plan_id
            where p.tenant_id = :authorized_tenant
              and (p.plan_state in ('OBSERVATION_REQUIRED',
                                    'RECOVERY_DECISION_REQUIRED',
                                    'COMPENSATION_PENDING',
                                    'MANUAL_RECOVERY')
                   or e.observed_state in ('RECORDED', 'DISPATCHING', 'ACCEPTED', 'UNKNOWN'))
            order by p.deadline_at, p.plan_id, e.step_number
            -- end::unresolved-recovery-query[]
            """;
}
