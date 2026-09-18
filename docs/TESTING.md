# AlpineJudge Testing Strategy


## Layer 1 — In-Container Agent (ajagent)

The ajagent is responsible for execution inside the sandbox.

Dedicated test harness included.

Current coverage:

* 9 Unit Tests
* 3 Integration Tests

Integration tests validate both successful execution and expected failure scenarios.

**Steps to run tests**
```bash
cd ajagent
go test -v ./...
```

---

## Layer 2 — Container Task Executor (runner)

Responsible for:

* Creation of continaer and containerd tasks
* Executing compile/run commands
* Collecting execution status
* Container cleanup

Validated using a dedicated container factory.
- Requires `sudo` because of interaction with containerd 
- Make sure to run `rm -rf /tmp/testcontainer-rmq.conf` after test since it doesn't cleanup automatically

**Steps to run tests**
```bash
cd runner
sudo go test -v ./...
```
---

## Layer 3 — API level (dispatcher)

Responsible for:

* Validate client request
* Create execution jobs 
* Send back live execution update via SSE to client

Testing covers:

* HTTP server
* Request validation
* RabbitMQ connectivity
* Configuration loading

**Steps to run tests**
```bash
cd dispatcher
go test -v ./...
```
---

# Design Philosophy

The testing strategy mirrors AlpineJudge's architecture.

Each layer is independently validated while tests progressively increase the scope of verification.

This separation provides several advantages:

* Fast identification of regressions
* Independent verification of architectural boundaries
* High confidence before releases
* Easier long-term maintenance

Testing is treated as a first-class engineering discipline rather than a post-development activity.
