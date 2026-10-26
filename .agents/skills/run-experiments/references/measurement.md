# Measurement and continuation

Closed-loop load is the sum of transaction slots across client processes. Open-loop load is a target new-transaction rate with a separate in-flight cap. Regular arrivals are the default; Poisson is explicit. At capacity the client waits and stops producing new requests; it does not accumulate arrivals, drop on overflow, or catch up with a burst. Compare actual achieved rate with the target and inspect capacity-wait diagnostics before describing the load as completion-independent.

Spanner prefers MPL=1 and additional client processes. Increase MPL only when process capacity is insufficient, up to eight. Machine count, process count, and MPL are separate; use the frozen exact allocation rather than multiplying assumptions.

Latency starts when execution actually begins and ends at terminal completion, including retries/backoff after execution begins. Exclude pre-send capacity waiting. Ignore transactions unfinished at measurement end; do not wait for them to enlarge the measurement population. Retain the named population and window for attempt-level rates.

Pool compatible raw samples or mergeable histograms across clients to compute percentiles. Averaging client p99s is invalid. CRDB restart, SQL retry, and final error counters remain distinct layers with explicit numerators and denominators; a restart-per-commit ratio need not be below 100 percent.

Analyze all batches associated with one logical experiment. The framework checks plan, configuration, source, artifact and environment compatibility, and deduplicates by logical cell/repeat identity. Conflicting successful records require resolution, not favorable-value selection. Distinguish common presentation from evidence of a same-batch off/on pair.

Workpoint selection is automatic from the frozen rule: use a point before severe latency degradation, with throughput near its plateau, then continue the declared followup. Keep selection reasons and candidate points. Invalid scans use the predeclared bounded extension/failure behavior. Do not tune the criterion after seeing the desired result or inherit an old 80%-of-knee rule.
