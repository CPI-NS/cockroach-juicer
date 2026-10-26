# Current evaluation cards

Under the installed `EF_SHARE` root from [commands](commands.md), read
`docs/evaluation/COVERAGE.md` and the requested entry in
`examples/evaluation/coverage.yaml`. They map all 18 cards to native config,
required bindings, raw evidence and figure capabilities. Templates live beside
coverage.yaml; site inputs live in sites/. Load only the requested card and site.

Use `$EF_SHARE/docs/implementation.md` for automatic
selection and fresh-data defaults. Use the maintained configuration contract.
Freeze numeric selection rules, scan bounds and followup declarations before
running. Scan each protocol/workload separately. A multi-protocol matrix is a
coverage example, not permission to select one workpoint from pooled protocols.

Bind the site, revisions, workpoint/dense grid, hold calibration and buffer formula
as required by that card. Named unresolved choices remain unresolved: a native
default or a validation-only site does not freeze a scientific choice.
In particular, T-CR-00/01 intentionally contain `measurement.stability: null`;
the real site must bind the full completion-bin policy and thresholds described
in the CRDB adapter documentation. A missing policy is a validation error, not
permission to disable the check or adopt plan-test fixture thresholds.

T-CC-08 uses the repaired RMW server and matching client/adapter with
`distinct_keys: false`. Build both off/on from the same corrected native source;
never substitute an older binary or silently deduplicate. The RMW skew exporter
analyzes compatible evidence; its fixtures do not establish native correctness. T-CC-12 is analysis-only and
consumes T-CC-05/07/11 evidence; never start a new load for it. A passing plan
test proves configuration translation, not collection, analysis or figure
completion. Distinguish implemented/exported capabilities from a completed
scientific campaign; report the actual missing scope without reopening repaired
capabilities from an older coverage snapshot.
