# CI listener-port collision diagnosis and scoped repair evidence

The CI fixture allocator used bind(0), closed that socket, then started listeners
later. Linux automatic allocation could return the same port repeatedly or let
another automatic listener take a selected port during this handoff. In the
isolated kernel reproduction, default ephemeral 32768–60999 plus CI reserved
49152–65535 yielded only 175 unique ports from 300 picks (32769 occurred 125 times),
and14 of 100 competing-listener trials caused actual EADDRINUSE (98). The probe ran in
an isolated user/network namespace and left host sysctls untouched. The retained
regression also checks that its network namespace differs before writing any
namespaced network sysctl. This report curation changes no sysctls or services.

Hosted run [39774960133](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39774960133)
independently selected 32771 for 5 of 6 cluster endpoints: all 3 S3 ports plus meta/data-a
internal ports; data-b internal used 41005. Both data-node service logs contain
an eaddrinuse marker and the recorded processes exited 1. The original restore
lane remains blocked. `hosted-cluster-start.json` and `hosted-restore-lane.json`
retain the exact small receipts. `hosted-diagnosis.json` keeps only selected
facts and private-log hashes/marker booleans; no raw service log is copied.

The shared helper now bind-checks explicit ports 1024–49151 outside the detected
Linux automatic ephemeral range, never reissues a selected port in-process, and
excludes the native journal's explicit 49152–65535 pool. Darwin uses sysctl range
discovery with conservative non-Linux fallback. Crashes, faults, resources,
backlog, restore and daily selectors share it. Continuously bound Python
proxy/sink/canary listeners retain bind(0) safely; fixed cluster ports already lie
outside the CI automatic range. No production listener reuseaddr behavior,
startup retry, resource envelope or 100 ms rule was changed. A separate explicit
binder can still race a closed probe socket; this change excludes automatic
client/distribution/listener selection and process-local duplicate selections.

`regressions.log` is the exact root-owned 21/21 passing test log. This includes the
isolated reserved-range regression, repeated/concurrent picks, competing automatic
listeners, native-journal exclusion and affected helper contracts. The log alone
does not record argv. `regression-command.json` contains the exact parent-reported
reconstruction, explicitly distinguished from an independently retained command. `runtime-commands.json`
retains the exact commands and terminal outcomes for the real fixed-port checks:

| Scope | Exit | Elapsed | Result |
| --- | ---: | ---: | --- |
| Whole-BEAM crash/restart |0|17.607s|All 11 publication boundaries passed |
| Cluster restore |0|29.839s|A37 data/fence/quarantine/restart/activation checks completed |

`fixed-crashes.json` retains all 11 boundary results. `fixed-restore.json`,
`fixed-restore-boundary.json` and `fixed-restore-isolation.json` retain the compact
recovery evidence, including three verified generation 2 voters, original roots
inaccessible, source processes stopped, and retained quarantine restart. Selected
source/recovered listener assignments are 12 distinct endpoints, all below 32768;
`fixed-listener-ports.json` retains the port maps and original receipt hashes.
Repeated start receipts deliberately keep the same assignments across restarts.
`shutdown-evidence.json` distinguishes recorded process exits/cleanup assertions
from an independent final OS PID/start inventory, which was not supplied here.
No all-owned-runtime-dead claim is fabricated from a successful command alone.

A subsequent dirty-tree fault run exited 0 in 11.489s. All eight checks passed,
including both upload disconnects, both downstream cancellations, upstream
interruption, restart, actual isolated ENOSPC/recovery and mailbox bounds. Both
online coverage windows have no failures. `fixed-faults.json` retains these
selected checks and coverage plus the original result hash; the command receipt
is `fault-runtime-command.json`. No separate final PID/start inventory or final
acceptance source identity is claimed for this run.

These checks ran from dirty intermediate source on `main` at
`4457631e4aef776e6106a086f21ab4e0bc1a0263`; a sibling's partial diagnostic directory
was held stable during execution. They are not immutable/final-source acceptance.
The crash receipt fenced fingerprint
`ac65a1b192dee251d960d6cb62bea975ba5d1f4bcc1b3f46a18b772759cf961a`;
the failed hosted restore recorded
`7dcef2598560de62da7dd12a889f7f4d3848d02b1d5905afd43211511eefe739`.
The restore command receipt has no independent source fingerprint. `provenance.json`
separates these execution identities from selected source-file hashes captured at
curation; later sibling diagnostic/workflow edits are outside the execution claim.

Original failing CI receipts remain unchanged. The valid 138940762 ns resource gap
in run 39768692842 still exceeds the unchanged 100000000 ns limit; filesystem versus
scheduler/off-CPU cause remains unresolved. A local diagnostic without that gap
and these port checks do not promote resource or complete hardening acceptance.
`remaining-gaps.json` preserves that boundary. No publication or final-candidate
acceptance is claimed. This report retains no raw private logs, credentials or
large sampling artifacts; copied small receipts remain byte-identical.
