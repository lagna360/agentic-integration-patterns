package dev.agenticintegrationpatterns.orderdesk.coordination;

public interface ParallelInvestigator {
    InvestigationReply investigate(
            InvestigationBranch branch,
            ParallelBranchRequest request);
}
