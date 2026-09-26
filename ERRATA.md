# Errata

This ledger records confirmed corrections to the companion source code for *Agentic Integration Patterns*, first edition, version 1.0. The book itself is not distributed from this repository.

| Date | Format / location | Correction | Status |
|---|---|---|---|
| 2026-09-26 | Companion, Chapter 7 context resolver | Sample acquisition time after the source returns; recheck every mandatory artifact and the run deadline before saving and returning model context, including on repeated operational resolution. Previously, an artifact could expire during acquisition yet be projected as fresh. Expired retained snapshots remain accessible through the separate read-only diagnostic path. | Corrected in this revision; moving-clock regression tests added. |
| 2026-09-26 | Companion, Chapter 8 capability gateway | Check observation freshness and run deadline when the tool result returns, and record that receipt time. Previously, the gateway could reject a valid new observation or accept an expired or late one. | Corrected in this revision; moving-clock regression tests added. |

Readers may report suspected, non-sensitive companion-code errors through this repository's GitHub Issues. Do not include credentials, private data, or security exploit details in a public issue; follow `SECURITY.md` instead.
