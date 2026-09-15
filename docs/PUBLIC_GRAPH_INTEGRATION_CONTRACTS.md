# Public graph integration surfaces

The shipping public-contract manifest explicitly declares
`okto_pulse.core.kg.logical_transfer`, the backend-neutral M-PULSE-5 package,
and the exact symbol `okto_pulse.core.kg.blocking_io.run_blocking_graph_io`.
Community already consumes both for graph transfer and cancellation-safe
synchronous graph dispatch. The declaration does not export the entire blocking
implementation module or any concrete backend/storage implementation.

The Community expected manifest remains checked against the Core manifest;
the mismatch guard and zero-private-reach-in audit are not bypassed. Validation
includes the manifest resolution tests, logical-transfer boundary checks and
blocking-I/O cancellation/context tests: 39 passed in 4.55 seconds. An earlier
manifest/transfer-validation slice also passed 69 tests; these slices overlap.
