## Concurrency and State Mechanics

**Stream Reader Lifecycle:** `stream.Reader` exposes no `Close` method. Reader release is bound exclusively to `NewReader` context cancellation, structurally eliminating concurrent state mutation paths.

**Buffer Closure and Ordering:** Buffer closure executes strictly after `cmd.Wait` returns and copying goroutines terminate, ensuring complete output ingestion. `Write` operations during process execution swallow errors (as `os/exec` treats write errors as fatal, dropping the exit status). Post-closure `Write` operations return `ErrWriteAfterClose`, explicitly rejecting late bytes rather than acknowledging discard.

**Termination State Machine:** `setTerminal` records job outcomes under the job's mutex. `STOPPED` and `DONE` states are keyed to the `stopRequested` boolean rather than the `Wait` error, as `os/exec` returns `ExitError` for all killed processes.

**Registry Locking:** RPCs resolve job state via a single `registry.Get` operation and pass the reference down the call stack, eliminating redundant lock acquisition.

**Process Group Reaping:** `reap` and `Stop` terminate the complete cgroup before destruction. `Process` exposes `Terminate` to handle process-group fallback signaling.

**Exit Code Extraction:** `Wait` error inference is removed. `Process` reads `ExitCode` directly from `ProcessState`. This prevents clean exits with expired `WaitDelay` or post-completion context cancellations from being misclassified as `-1` signal deaths. 

**Error Classification (`EINVAL`):** `argv` is pre-flighted for null bytes before `fork/exec`. Kernel `EINVAL` rejections for missing `CLONE_INTO_CGROUP` support are classified as server-side faults and logged, rather than surfacing to clients as bad requests.

**gRPC Buffer Allocation:** Output chunk buffers are allocated per iteration to prevent `Read` overwrites caused by lazy reading in `grpc-go` tracing and stats handlers.

**Buffer Simplification:** The lazy channel initialization is replaced with a single persistent broadcast channel, swapped (closed and recreated) on every append/close. `data`, `closed`, and `changed` state variables are guarded by a single mutex, which is never held across `select`/wait boundaries.

## Testing and CI Infrastructure

**Concurrency Stress Testing:** API stress tests execute N concurrent goroutines under `go test -race -count=N` with varied `GOMAXPROCS` to force thread interleaving.

**Leak and Deadlock Detection:** Test binaries integrate `go.uber.org/goleak` to enforce strict goroutine termination and `sasha-s/go-deadlock` to trap lock-order inversions.

**CI Pipeline:** The CI workflow executes `golangci-lint`, `go vet`, and `go test -race` on every push.

## Formal Verification (TLA+/PlusCal)

System protocols are modeled in PlusCal/TLA+ and evaluated continuously in CI using `tla2tools.jar` (TLC). 

**Termination Protocol:** Models the Stop caller, process exit/kill, and reaper. TLC validates monotonic state transitions (forbidding `RUNNING` after terminal state), verifies terminal cause accuracy, and ensures no reader is left parked.

**Output Buffer:** Models a bounded-length buffer with 2 readers and 1 writer. TLC validates safety (reads never observe unsynchronized writes; EOF propagates exactly once) and liveness (fairness constraints ensure parked readers wake).

**Ordering Invariant:** TLC proves `Close` strictly happens-after the final `Write`.

## Identity and Cryptographic Execution

**SVID Lifecycle:** A dedicated issuer and Workload-API endpoint mint short-lived SVIDs, replacing static one-year certificates. The serving identity rotates independently without terminating active `Output` streams.

**Per-Job Identity:** The executor provisions a distinct SPIFFE ID and SVID for every spawned job, fully decoupling workload identity from the caller identity.

**Delegation and Attenuation:** The executor enforces cryptographic attenuation via offline tokens (Macaroons/Biscuit) or RFC 8693 token exchange. Jobs execute with a strictly bounded subset of the caller's authority.

**Revocation:** Short-lived SVID issuance acts as the primary revocation mechanism.

## Authorization and Policy Engine

**Externalized OPA/Rego:** Hardcoded `authz.Policy` checks are replaced with an OPA/Rego engine. Authorization decisions map to SPIFFE identity attributes and job labels.

**Execution Authorization:** Workload policies enforce command allowlists and resource ceilings based on the caller's identity. 

**Fail-Closed Semantics:** The API returns `NOT_FOUND` instead of `PERMISSION_DENIED` across all policy rejections to prevent resource enumeration.

## Audit Logging

**Cryptographic Ledger:** Every start, stop, and view operation is signed by the executing identity and appended to a tamper-evident, hash-chained ledger.

## Isolation and Resource Constraints

**Privilege Dropping:** The executor drops privileges to a distinct unprivileged UID/GID prior to exec. The credential struct is explicitly configured to clear all supplementary groups.

**Namespaces:** The execution context requires a PID namespace to guarantee kernel-driven descendant cleanup and a network namespace to enforce network isolation.

**Buffer Eviction:** The output buffer implements count-based eviction to purge completed jobs exceeding the retention threshold.

**Resource Quotas:** `cpuMax`, `memoryMax`, `memorySwapMax`, `ioBandwidth`, and `pidsMax` are configurable, per-identity quotas rather than hardcoded constants. `memory.high` is configured as a soft threshold to throttle transient allocation spikes before triggering the `memory.max` OOM-killer.

**I/O Throttling:** `io.max` configuration supports multi-disk and LVM/dm layouts, scaling beyond the legacy single `major:minor` device limitation.

## Federation

**Cross-Domain Trust:** The executor ingests exchanged trust bundles to authenticate workloads from federated SPIFFE trust domains.

