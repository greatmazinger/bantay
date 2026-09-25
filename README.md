# bantay

*bantay*, Tagalog for guard, runs, isolates, and watches processes on behalf of
authenticated clients.

A gRPC service that runs arbitrary Linux processes for authenticated clients and
streams their output. Each job runs under its own cgroup v2 resource limits, and
its output is streamed to any number of concurrent clients from the moment the
process started.

The design and the reasoning behind it are in
[bantay-design-doc.md](bantay-design-doc.md), which is the document to read if
you want to know why anything here is the way it is.

Output delivery is the central design problem. A client may attach before a job
has produced anything, midway through, or after it has exited, and every client
must receive the same complete byte stream from the beginning. Isolation, by
contrast, is mostly the kernel's work: the job's cgroup is created before the
process exists and the descriptor is handed to `clone` so there is no window in
which a job runs unconstrained.

## What is here

| Package | What it does |
|---|---|
| `pkg/runner/stream` | The output buffer: append-only, many concurrent readers, each receiving the complete stream from the first byte written |
| `pkg/runner` | The job library: start a process, stop it and every descendant, query its state, stream its output. Knows nothing about gRPC |
| `internal/cgroup` | One cgroup v2 group per job: resource limits, termination, removal |
| `internal/authz` | Who may act on a job: its owner, or an administrative identity |
| `internal/authn` | Who the caller is: the SPIFFE ID in the client certificate |
| `internal/tlsconfig` | TLS 1.3, mutual authentication, one certificate authority |
| `internal/idgen` | Job identifiers |
| `internal/server` | The translation layer: gRPC in, library calls out, domain errors mapped to status codes |
| `proto/jobmanager/v1` | The wire schema and its generated code |
| `cmd/jobworker` | The server. The only place that decides policy: limits, administrators, where the cgroup subtree and state directory live |
| `cmd/jobctl` | The client. One subcommand per RPC |
| `cmd/gencerts` | Issues the CA, the server certificate, and three client identities |

## Requirements

To build and test:

- Go 1.26 or later.
- Linux. `pkg/runner` sets `SysProcAttr.UseCgroupFD` to place a process inside
  its cgroup at creation, and that field exists only on Linux, so the package
  does not compile elsewhere.

The tests need nothing further: they run against ordinary temporary directories
and a mock process, and assert that the service writes the values it says it
writes rather than that the kernel acts on them.

To actually enforce limits:

- cgroup v2, with the unified hierarchy mounted at `/sys/fs/cgroup`.
- Kernel 5.7 or later for `CLONE_INTO_CGROUP`, 5.14 or later for `cgroup.kill`.
- A cgroup v2 subtree delegated to the server's user, with `cpu`, `memory`,
  `io` and `pids` enabled in its `cgroup.subtree_control`, and the server
  started inside it. The design document explains why all three are required under Environment
  assumptions.

The server refuses to start if a controller it needs is not delegated, or if the
device backing the state directory cannot be throttled. Without a cgroup root it
runs without limits and says so.

## Build and test

    make build     # go build ./...
    make test      # go test -race -count=1 ./...
    make vet       # go vet ./...
    make check     # vet and test

`go build ./...` is the entire build: no code generation, no tooling to install.
`make test` runs with the race detector and without the test cache, which is how
the concurrency tests are meant to be run.

## Running it

Issue certificates, start the server, use the client. Three client identities
are issued: `alice`, `bob`, and `alice-admin`. Two exist so that authorization
can be demonstrated, and `alice-admin` belongs to the same person as `alice`,
which is the intended way to hold the administrative role: reach for the second
certificate only when acting on someone else's job.

    go build -o bin/ ./cmd/jobworker ./cmd/jobctl ./cmd/gencerts
    ./bin/gencerts                          # writes ./certs
    ./bin/jobworker                         # listens on :8443, jobs run in ./state

Each job gets its own directory under the state directory, removed when the job
is reaped. `--state-dir` names it and defaults to `./state`; job directories
left by a server that died without reaping are cleared at startup, and anything
else found there is left alone and logged.

Then, in another shell:

    export JOBCTL_IDENTITY=alice

    ./bin/jobctl start -- echo hello        # prints the job ID
    ./bin/jobctl status job-1-3f2a          # state=DONE  exit=0
    ./bin/jobctl output job-1-3f2a          # hello
    ./bin/jobctl stop job-7-b91c

`start` prints the job ID and nothing else, so it composes:

    ./bin/jobctl output "$(./bin/jobctl start -- ./build.sh)"

Everything after `--` is passed through as argv with no shell interpretation, so
`jobctl start -- echo '$HOME'` prints the literal string. The separator is
required rather than guessed: without it, a job's flags and `jobctl`'s own are
indistinguishable.

`jobctl output` blocks, writing bytes to stdout as they arrive with no framing or
line buffering, because a job's output may be binary. Interrupting it with Ctrl-C
ends the stream and leaves the job running. The client is a viewer, not an owner,
and detaching is not the same as stopping.

`jobctl` exits zero when the RPC succeeded and non-zero when it failed. It does
not adopt the job's exit code: that would make a successful query of a failed job
indistinguishable from a failed query. The job's own result is in `status`.

`--server`, `--certs-dir` and `--identity` cover the client's side of the same
choices; `./bin/jobctl help` lists them.

## Demonstrating concurrent delivery

`make harness` runs the property the design is built around, without a server
or certificates: it starts one job and attaches 1,600 readers to it.

    make harness        # or: go run ./cmd/harness

Five hundred attach before the job writes anything, five hundred at staggered
points while it runs, five hundred after it has exited, and one hundred walk
away partway through by cancelling their context. Every reader that stayed must
receive a byte-identical stream, the ones that left must come back with
`context.Canceled` and a prefix of that same stream, and the stream must reach
the job's final write.

That last check matters more than it looks: without it the harness only proves
the readers agree with each other, which a bug that truncates the stream for
everyone satisfies just as well as correct delivery.

## Running it with isolation

Limits are only enforced when the server runs inside a delegated cgroup v2
subtree, which it cannot arrange for itself: a process started outside a subtree
cannot move into it, because migrating between cgroups needs write access to the
`cgroup.procs` of the common ancestor, and for a process started elsewhere that
ancestor is the root cgroup. So the sequence has to be place, drop privilege,
then exec.

`scripts/run-delegated.sh` does exactly that. It is the systemd-free equivalent
of a unit with `User=` and `Delegate=yes`, and it needs root only to establish
the delegation: the program it execs runs as an ordinary user with no privilege
of its own.

    sudo RUN_AS="$USER" scripts/run-delegated.sh \
      ./bin/jobworker \
      --cgroup-root /sys/fs/cgroup/jobworker \
      --state-dir /var/lib/jobworker

`CGROUP_ROOT` and `SUBTREE` override where the subtree is created; they default
to `/sys/fs/cgroup` and `jobworker`.

With a cgroup root the server checks its preconditions before serving: every
controller it writes must be enabled in the parent's `cgroup.subtree_control`,
and the device backing the state directory must accept an `io.max` write. It
refuses to start otherwise, naming what is missing, rather than starting and
running jobs unconstrained.
