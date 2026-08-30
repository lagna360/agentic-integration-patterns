package dev.agenticintegrationpatterns.orderdesk.history;

import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Set;

/** Deterministic fixture that proves manifest binding and isolation, not live-model reproducibility. */
@Component
public class DeterministicReplayEvaluator implements ReplayEvaluator {
    @Override
    public boolean isAvailable(String mode, ReplayInputManifest manifest) {
        return "RECONSTRUCT".equals(mode)
                || ("REEVALUATE".equals(mode)
                    && Objects.equals("model:fixture-v1", manifest.modelRef()));
    }

    @Override
    public Result reconstruct(ReplayInputManifest manifest) {
        return result(ResultCode.RECONSTRUCTED, "reconstruct", manifest,
                Set.of(GapCode.INPUT_BYTES_NOT_REEXECUTED));
    }

    @Override
    public Result reevaluate(ReplayInputManifest manifest) {
        if (!isAvailable("REEVALUATE", manifest))
            throw new IllegalStateException("PINNED_IMPLEMENTATION_UNAVAILABLE");
        return result(ResultCode.REEVALUATED, "reevaluate", manifest,
                Set.of(GapCode.FIXTURE_EVALUATOR));
    }

    private Result result(ResultCode code, String mode, ReplayInputManifest manifest,
            Set<GapCode> gaps) {
        return new Result(code, ReplayInputManifest.sha256(
                mode + "\n" + manifest.manifestSha256()), gaps);
    }
}
