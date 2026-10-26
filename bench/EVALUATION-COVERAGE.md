# CRDB evaluation capabilities

The maintained adapter supplies SQL YCSBT execution and the analysis interfaces
below. Capability availability is distinct from a successful scientific campaign.
Use [EVALUATION.md](EVALUATION.md) for configuration and
[VALIDATION.md](VALIDATION.md) for verification procedures.

| Capability | Implementation and boundary |
|---|---|
| Hot-rank layout | Discover node/store identities, split hot keys, check voters and place leases with SQL receipts |
| Runtime layout checks | Observe placement/settings through measurement and finalization; snapshots cannot exclude moves between samples |
| Hold calibration | Observe nominated-key spans without holding/queueing; merge complete native epochs within the client-window intersection |
| Calibration validity | Check zero queueing/holds, missing spans and histogram overflow; unresolved/read-only spans are not imputed |
| KV retry layers | Report named restart reasons, CC subset, uncertainty and their declared server denominator |
| SQL retry text | Report classifications observed at automatic-retry increments and reset/export boundaries; text is not structured causal identity |
| Client errors | Separate visible 40001, terminal failures and deadlocks using their declared populations |
| 1PC | Report native success/failure counters and failures divided by total attempts |
| Stability | Explicit configured thresholds, completion bins, p99 series and insufficient/unstable states |
| Knee selection | Exclude unsafe/invalid loads and preserve off-only stability stop behavior across continuation |
| Figure inputs | Export state-labelled throughput/latency data and source/environment/repeat identities |

The shared Juicer core supplies observe-only calibration. Server/key spans end
when commit/abort is applied there; individual start times and read-only terminal
events are not inferred. A finite reported p99 is required for a hold-cap choice.

Read [SQL-RETRY-TEXT-OBSERVATION.md](SQL-RETRY-TEXT-OBSERVATION.md) before using
text classes. SQL totals, KV causes, histogram populations, 1PC and visible errors
remain separate. Matching totals do not prove identical causes.

Bind all scientific thresholds in the reviewed configuration. Native execution,
external SSH, installation, platform support and every evaluation card need their
own evidence; synthetic fixtures establish parser behavior only.
