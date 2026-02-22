# CRITICAL Findings

4 findings across 3 streams.

| # | Stream | Finding File | Title | Status |
|---|--------|-------------|-------|--------|
| 07-00 | 07-Dependency & Unsafe Code | `audit/07-dependency-unsafe-code-findings/00-no-catch-unwind-panic-safety.md` | No `catch_unwind` -- Panics in FFI Are UB | CONFIRMED |
| 02-02 | 02-Access Control Policy | `audit/02-access-control-policy-findings/02-commits-query-bypasses-acp.md` | _commits queries bypass ACP entirely | CONFIRMED |
| 03-12 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/12-two-stream-no-signature-verification.md` | Two-stream handler accepts messages without signature verification | CONFIRMED |
| 03-20 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/20-access-mode-controlled-never-activated.md` | AccessMode::Controlled is never activated -- collection access control is dead code | CONFIRMED |
