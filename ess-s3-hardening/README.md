# ESS S3 Hardening — Codex Execution Package

This package defines one implementation goal covering all three remediation phases for `gsmlg-opt/ex_storage_service`.

| File | Use |
|---|---|
| `CODEX_GOAL.md` | The implementing agent's execution brief and completion rule |
| `IMPLEMENTATION_PLAN.md` | Nineteen work packages across three phases, with architecture constraints and acceptance gates |
| `ACCEPTANCE_MATRIX.md` | Thirty-eight mandatory automated verification scenarios, including resource and recovery profiles |

All implementation and test statuses start pending. These documents are not a claim that the source repository has been changed or that the tests have run.

## Start

Extract the `ess-s3-hardening/` directory into the repository checkout, or give all three main Markdown files to Codex as task inputs. Then use this instruction:

> Read `ess-s3-hardening/CODEX_GOAL.md`, `ess-s3-hardening/IMPLEMENTATION_PLAN.md`, and `ess-s3-hardening/ACCEPTANCE_MATRIX.md`. Execute goal ESS-S3-HARDENING-001 in this repository. Implement and verify all three phases in order; do not stop after Phase 1 or return another plan. Maintain repository-local progress and evidence, preserve the documented architecture and safety guards, and report completion only when every mandatory acceptance scenario passes. Start with the actual checkout and baseline checks, then implement.

Adjust input paths only when the files were placed elsewhere. The goal also explains how to preserve progress in repository-local documents for resumed sessions.

The task authorizes local implementation and testing, not production deployment, publishing, pushes, or destructive operations on live data.
