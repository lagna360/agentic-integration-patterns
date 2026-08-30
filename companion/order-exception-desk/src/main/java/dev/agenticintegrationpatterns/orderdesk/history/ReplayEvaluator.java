package dev.agenticintegrationpatterns.orderdesk.history;

import java.util.Set;

/** Read-only computation port. Deliberately has no tool, message-publication, or effect capability. */
public interface ReplayEvaluator {
    /** Pure local capability check; it must not perform or charge for an evaluation. */
    boolean isAvailable(String mode, ReplayInputManifest manifest);

    Result reconstruct(ReplayInputManifest manifest);
    Result reevaluate(ReplayInputManifest manifest);

    record Result(ResultCode resultCode, String sha256, Set<GapCode> explicitGaps) {
        public Result {
            if (resultCode == null) throw new IllegalArgumentException("resultCode");
            if (sha256 == null || !sha256.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("sha256");
            explicitGaps = explicitGaps == null ? Set.of() : Set.copyOf(explicitGaps);
        }
    }

    enum ResultCode { RECONSTRUCTED, REEVALUATED }

    enum GapCode {
        FIXTURE_EVALUATOR,
        INPUT_BYTES_NOT_REEXECUTED,
        EVALUATION_OUTCOME_UNKNOWN
    }
}
