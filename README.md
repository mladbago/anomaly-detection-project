# Payment-Card Anomaly Detection

A real-time fraud-detection pipeline for payment-card transactions, built on
**Kafka + Apache Flink (PyFlink) + MongoDB** and visualised with a live
**Streamlit** dashboard. A simulator fabricates traffic for 10,000 virtual cards
— interleaving normal spending with deliberately injected anomalies — and a
stateful Flink job scores every transaction against six statistical rules,
emitting fraud alerts in near real-time.

The whole stack comes up with **one command** and exposes four web UIs.

---

## Table of contents

1. [Architecture](#architecture)
2. [Components](#components)
3. [Quick start (one command)](#quick-start-one-command)
4. [Web interfaces](#web-interfaces)
5. [The Streamlit dashboard](#the-streamlit-dashboard)
6. [Statistical detection rules](#statistical-detection-rules)
7. [Monitoring & health checks](#monitoring--health-checks)
8. [Testing guide](#testing-guide)
9. [Project layout](#project-layout)
10. [Configuration](#configuration)
11. [Teardown & troubleshooting](#teardown--troubleshooting)

---

## Architecture

```
                            PAYMENT-CARD ANOMALY DETECTION — SYSTEM ARCHITECTURE

  ┌──────────────────────┐
  │   SIMULATOR /         │   10,000 virtual cards. ~8% of ticks inject a random
  │   KAFKA PRODUCER      │   anomaly. Every payload carries the ground-truth label
  │   (simulator/)        │   is_injected_anomaly + expected_anomaly (JSON).
  └──────────┬───────────┘
             │ produce JSON
             ▼
   ╔═════════════════════════════════════════════════════════════════════════╗
   ║                          APACHE KAFKA  (KRaft, no ZooKeeper)             ║
   ║                                                                          ║
   ║   topic: payment_transactions ───────────────┐      topic: transaction_alerts
   ╚═══════════════┬══════════════════════╤════════╪══════════════════▲═══════╝
                   │                      │        │                  │
        consume    │            consume   │        │ consume          │ produce alerts
                   ▼                      ▼        ▼                  │
        ┌────────────────────┐   ┌──────────────────────┐   ┌────────┴───────────┐
        │  TEST CONSUMER     │   │   STREAMLIT          │   │  APACHE FLINK      │
        │  (ASCII console    │   │   DASHBOARD          │   │  FRAUD DETECTOR    │
        │  stream check)     │   │   (dashboard/)       │   │  (flink_detector/) │
        │  test_consumer/    │   │   :8501              │   │  JobManager :8081  │
        └────────────────────┘   │                      │   │  + TaskManager     │
                                 │  • live metrics      │   │                    │
                                 │  • live map          │   │  Keyed per card_id │
                                 │  • live charts       │   │  6 stateful checks │
                                 │  • live feeds        │   │  (Z-score, Haver-  │
                                 │                      │   │  sine, windows…)   │
                                 └───────▲──────┬───────┘   └────────┬───────────┘
                                         │      │ read                │ alerts
                                  read   │      │ live tx             ▼
                                  alerts │      │            ┌────────────────────┐
                                  history│      │            │  ALERT MANAGER     │
                                         │      │            │  (alert_manager/)  │
                                         │      │            │  consume alerts,   │
                                         │      │            │  notify + persist  │
                                         │      │            └─────────┬──────────┘
                                         │      │                      │ insert
                                         │      ▼                      ▼
                                 ╔═══════╧══════════════════════════════════════╗
                                 ║                 MONGODB                       ║
                                 ║       db: fraud_detection / coll: alerts      ║
                                 ╚═══════════════════════════════════════════════╝

   WEB UIs:   Streamlit :8501   ·   Flink :8081   ·   Kafdrop :9000   ·   Mongo-Express :8082
```

**Data flow in one line:** `simulator → payment_transactions → Flink (6 rules) →
transaction_alerts → alert-manager → MongoDB`, with the Streamlit dashboard and
the test-consumer tapping the topics for live visualisation.

---

## Components

| Component | Directory | Role |
|---|---|---|
| **Transaction simulator / producer** | `simulator/` | Generates JSON transactions for 10,000 cards and injects six anomaly types. Publishes to `payment_transactions`. Each payload is labelled with `is_injected_anomaly` / `expected_anomaly` (ground truth for testing). |
| **Flink fraud detector** | `flink_detector/` | PyFlink job, keyed by `card_id`. Runs six stateful statistical checks and emits alerts to `transaction_alerts`. Detection maths lives in the PyFlink-free `rules.py` so it is independently testable. |
| **Alert manager** | `alert_manager/` | Consumes `transaction_alerts`, prints notifications, and persists every alert (with its embedded transaction) into MongoDB. |
| **Streamlit dashboard** | `dashboard/` | Containerised live UI on **:8501** — metrics, map, charts, feeds. Taps both Kafka topics via a background thread and reads alert history from MongoDB. |
| **Test consumer** | `test_consumer/` | Console ASCII dashboard over `payment_transactions` to eyeball data correctness (`docker logs test-consumer`). |
| **Shared library** | `common/` | Config, constants, Haversine geo math, and the canonical transaction **JSON schema**. |
| **Test suite** | `tests/` | Schema-validation, rule unit tests, state-reset tests, offline precision/recall, and live MongoDB integration tests. |
| **Infrastructure** | `docker-compose.yml` | Kafka (KRaft), topic init, MongoDB, Flink JobManager/TaskManager + auto job submitter, Kafdrop, Mongo-Express, and all app services. |

> **No local JAR files.** `flink_detector/Dockerfile` pulls the Flink Kafka
> connector fat-jar straight into `/opt/flink/lib` with `curl` at build time —
> nothing binary is committed to the repo.

---

## Quick start (one command)

**Prerequisites:** Docker Engine + Docker Compose v2. ~4 GB free RAM.

```bash
docker compose up --build -d
```

That single command builds the custom PyFlink image, starts Kafka, creates the
topics, launches MongoDB, brings up the Flink cluster, **auto-submits the fraud
job**, and starts the simulator, both consumers, and the Streamlit dashboard.

Watch it come alive:

```bash
docker compose ps                 # all services should be Up / healthy
docker compose logs -f simulator  # see transactions (and "--- Injecting anomaly ---")
docker compose logs -f alert-dashboard   # see [ALERT] lines as fraud is caught
```

Give it ~60–90 seconds after startup for the Flink job to register and for the
first alerts to flow, then open the dashboard at **http://localhost:8501**.

---

## Web interfaces

| UI | URL | Purpose |
|---|---|---|
| **Streamlit dashboard** | http://localhost:8501 | Live fraud-monitoring dashboard (the main view). |
| **Flink UI** | http://localhost:8081 | Confirm the `payment-fraud-detector` job is RUNNING; inspect throughput. |
| **Kafdrop** | http://localhost:9000 | Browse Kafka topics, partitions, and raw messages. |
| **Mongo-Express** | http://localhost:8082 | Browse the `fraud_detection.alerts` collection (login `admin` / `admin`). |

---

## The Streamlit dashboard

`http://localhost:8501` — the dashboard auto-refreshes (interval configurable in
the sidebar) and shows:

- **Live metric counters** — total transactions, approximate normal traffic,
  injected anomalies seen, **alerts caught**, and the alert rate %.
- **Live map** — `st.map` plotting recent transaction locations (🔵) and
  `gps_jump` / impossible-travel alert locations (🔴) across Poland.
- **Live charts** — transaction & alert **volume over time** (per-second line
  chart) and an **anomaly breakdown by type** bar chart.
- **Live feeds** — tables of the most recent transactions and the most recent
  alerts, including the ground-truth `injected` label for at-a-glance validation.
- **Persisted alerts** — a MongoDB-backed totals table in the sidebar.

**How it works:** Streamlit reruns its script on every refresh, which is hostile
to a long-lived Kafka consumer. So `dashboard/stream_state.py` keeps a single
`StreamState` per server process (via `@st.cache_resource`) that owns a daemon
thread tailing both topics and folding messages into lock-guarded aggregates; the
UI only ever reads cheap snapshots.

---

## Statistical detection rules

All six rules live in `flink_detector/rules.py` as pure functions and are tuned
by `common/constants.py`. The Flink job (`flink_detector/job.py`) holds the
per-card state and calls these rules in a fixed order.

| Alert type | Rule | Statistic / method | Threshold (default) |
|---|---|---|---|
| `AMOUNT_SPIKE` | Charge is far above the card's own norm | **Rolling Z-score** via Welford's online `(count, mean, M²)` in keyed state | `z ≥ 3.5` after a warm-up of `≥ 8` samples |
| `IMPOSSIBLE_TRAVEL` | Two charges too far apart, too fast | **Haversine** distance ÷ time = implied speed | `speed > 1000 km/h` **and** `distance ≥ 50 km` |
| `FREQUENCY_BURST` | Too many charges in a short span | **Sliding count window** (keyed counter that resets when the window elapses) | `≥ 5` charges within `2000 ms` |
| `CARD_TESTING` | Probe-then-strike bot pattern | Sequence rule: micro-charge immediately followed by a big charge | `last < $1.00` **and** `current > $500` |
| `MAX_OUT` | Drains the card in one shot | Ratio of charge to available limit | `value ≥ 95%` of `available_limit` |
| `NIGHT_OWL` | Large charge at an unusual hour | Local hour-of-day + value gate | `value > $1000` **at local hour 3** |

### Why these three are highlighted

- **Rolling Z-score (`AMOUNT_SPIKE`).** Each card builds its *own* spending
  baseline. Welford's algorithm keeps only three numbers in state — count, mean,
  and M2 (sum of squared deviations) — so we never store history yet can compute
  `z = (value − mean) / std` exactly. A charge ≥ 3.5σ above the card's mean is
  flagged. A warm-up guard (≥ 8 samples) prevents a thin baseline from inventing
  outliers.
- **Haversine speed limit (`IMPOSSIBLE_TRAVEL`).** The great-circle distance
  between consecutive transaction GPS coordinates, divided by the elapsed time,
  yields an implied travel speed. Exceeding 1000 km/h *and* a meaningful 50 km
  distance (so tiny GPS drift can't trip it) implies the card is in two places at
  once.
- **Sliding window burst count (`FREQUENCY_BURST`).** A per-card counter tracks
  charges inside a 2-second window and **resets the moment the window elapses**,
  so a slow trickle never accumulates into a false burst — 5+ hits inside one
  window is the signal.

---

## Monitoring & health checks

**Are all services healthy?**

```bash
docker compose ps
```

**Kafka — do the topics exist and have data?**

```bash
# list topics
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# tail live transactions
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic payment_transactions --max-messages 5

# tail live alerts
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic transaction_alerts --max-messages 5
```

**Flink — is the detector job running?**

```bash
# REST overview (expects "jobs-running": 1)
curl -s http://localhost:8081/overview | python3 -m json.tool

# list jobs and their status
curl -s http://localhost:8081/jobs | python3 -m json.tool
```

…or just open the Flink UI at http://localhost:8081 and confirm
`payment-fraud-detector` is **RUNNING**.

**MongoDB — is it up and receiving alerts?**

```bash
# ping
docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping')"

# count alerts
docker exec mongodb mongosh --quiet fraud_detection \
  --eval "db.alerts.countDocuments({})"
```

---

## Testing guide

The project ships a layered testing strategy that proves the pipeline processes
data **correctly** and that the statistical algorithms are **accurate**. The
unit/offline tests need no running infrastructure; the integration tests validate
the live stack.

### Run the offline suite

```bash
# with the project venv (jsonschema is the only hard dep for these)
python tests/run_all.py            # zero-dependency runner

# …or with pytest
pip install -r tests/requirements.txt
pytest tests/ -v
```

Expected: the schema, rule, state, and precision tests **pass**; the live
MongoDB integration tests **skip** until the stack is running.

### 1. Validation test — does the simulator's JSON match what Flink expects?

`tests/test_schema_validation.py` answers this two ways:

1. Every payload the simulator emits (normal **and** all six anomaly types) is
   validated against the canonical contract in `common/schema.py`
   (`TRANSACTION_SCHEMA`, JSON-Schema Draft 7).
2. The schema's `required` set is asserted to cover **exactly the fields the
   Flink job dereferences** (`FLINK_REQUIRED_FIELDS`). If someone reads a new
   field in `job.py` without declaring it in the contract, the build fails.

This guarantees the producer and the consumer can never silently drift apart.

### 2. Precision test — is the engine missing anomalies or crying wolf?

The simulator labels every fabricated charge with `is_injected_anomaly: true` and
the rule it targets in `expected_anomaly`. Because the Flink alert envelope embeds
the full transaction, those labels ride all the way into MongoDB — giving us
ground truth to score against.

**Offline (`tests/test_precision_offline.py`, deterministic, no infra):** streams
simulator output through a `ReferenceDetector` — a PyFlink-free twin that calls
the *same* `rules.py` functions in the *same* order as the Flink job — and asserts:

- **Precision = 100%:** every alert lands on an `is_injected_anomaly` charge
  (a flagged *normal* charge would be a false positive), and
- **Recall:** all six injected anomaly types are detected.

**Live (on the running stack):** query MongoDB directly. A false positive is any
stored alert whose embedded transaction was *not* an injected anomaly:

```bash
docker exec mongodb mongosh --quiet fraud_detection --eval '
  const total = db.alerts.countDocuments({});
  const fp    = db.alerts.countDocuments({ "transaction.is_injected_anomaly": { $ne: true } });
  print("alerts:", total, " false positives:", fp,
        " precision:", total ? ((total - fp) / total).toFixed(4) : "n/a");
'
```

Break it down by detected type, and cross-check against what the simulator
*intended*:

```bash
# detected alerts grouped by type
docker exec mongodb mongosh --quiet fraud_detection --eval '
  db.alerts.aggregate([
    { $group: { _id: "$alert_type", count: { $sum: 1 } } },
    { $sort: { count: -1 } }
  ]).forEach(d => print(d._id.padEnd(20), d.count));
'

# do Flink's alerts line up with the simulator's expected_anomaly label?
docker exec mongodb mongosh --quiet fraud_detection --eval '
  db.alerts.aggregate([
    { $group: { _id: { fired: "$alert_type",
                       expected: "$transaction.expected_anomaly" },
                count: { $sum: 1 } } },
    { $sort: { count: -1 } }
  ]).forEach(d => print(JSON.stringify(d._id).padEnd(60), d.count));
'
```

The same assertions are automated in `tests/test_pipeline_integration.py`
(run `pytest tests/test_pipeline_integration.py -v` after the stack has gathered
a few alerts).

### 3. State test — does Flink's temporary memory reset correctly?

`tests/test_state_reset.py` drives the keyed-state orchestration (via the faithful
`ReferenceDetector` twin) to prove the per-card temporary memory behaves:

- the **frequency-burst counter resets to zero** the instant a new 2-second
  window opens, so a slow trickle of charges never accumulates into a false
  burst (`window_should_reset` is the rule under test);
- five charges packed inside one window **do** trip the burst;
- state is **isolated per card** — one card's burst never contaminates another;
- the Welford Z-score accumulator stays well-formed (M² ≥ 0) as samples fold in.

### Test map

| Requirement | File |
|---|---|
| Validation (schema ↔ Flink contract) | `tests/test_schema_validation.py` |
| Statistical rule boundaries | `tests/test_rules.py` |
| State reset / temporary memory | `tests/test_state_reset.py` |
| Precision & recall (offline) | `tests/test_precision_offline.py` |
| Precision & persistence (live MongoDB) | `tests/test_pipeline_integration.py` |

---

## Project layout

```
anomaly-detection-project/
├── docker-compose.yml          # one-command stack (incl. Streamlit :8501)
├── common/                     # shared config, constants, geo math, JSON schema
│   ├── config.py
│   ├── constants.py
│   ├── geo.py                  # Haversine
│   └── schema.py               # canonical transaction contract
├── simulator/                  # transaction generator + Kafka producer
│   ├── generator.py            # normal traffic + 6 injected anomaly types (labelled)
│   └── producer.py
├── flink_detector/             # PyFlink fraud job
│   ├── job.py                  # stateful keyed orchestration + Kafka source/sink
│   ├── rules.py                # PyFlink-free statistical rules (unit-testable)
│   └── Dockerfile              # pulls Kafka connector jar via curl (no local jars)
├── alert_manager/              # consumes alerts → notify + persist to MongoDB
│   └── notifier.py
├── test_consumer/              # ASCII console stream check
│   └── consumer.py
├── dashboard/                  # Streamlit live dashboard (NEW)
│   ├── app.py                  # metrics, map, charts, feeds
│   ├── stream_state.py         # background Kafka tap + thread-safe state
│   └── Dockerfile
└── tests/                      # validation / rule / state / precision / integration
    ├── reference_detector.py   # PyFlink-free twin of the Flink detector
    └── run_all.py              # zero-dependency runner
```

---

## Configuration

Tuning knobs live in `common/constants.py`:

| Constant | Meaning | Default |
|---|---|---|
| `TOTAL_CARDS` | Number of virtual cards | `10000` |
| `ZSCORE_THRESHOLD` | Std-devs above mean for `AMOUNT_SPIKE` | `3.5` |
| `ZSCORE_MIN_SAMPLES` | Warm-up before Z-score is trusted | `8` |
| `MAX_SPEED_KMH` | Speed limit for `IMPOSSIBLE_TRAVEL` | `1000.0` |
| `IMPOSSIBLE_TRAVEL_MIN_KM` | Minimum jump distance to flag | `50.0` |
| `FREQUENCY_BURST_THRESHOLD` / `FREQUENCY_WINDOW_MS` | Burst count / window | `5` / `2000` |
| `MAX_OUT_RATIO` | Fraction of limit for `MAX_OUT` | `0.95` |
| `SPIKE_MIN_VALUE` / `MICRO_CHARGE_MAX` | `CARD_TESTING` bounds | `500.0` / `1.0` |
| `NIGHT_OWL_MIN_VALUE` / `NIGHT_OWL_HOUR` | `NIGHT_OWL` value / hour | `1000.0` / `3` |

Connection settings (`common/config.py`) are environment-overridable:
`KAFKA_BOOTSTRAP_SERVERS`, `MONGO_URI`. Inside Compose they default to
`kafka:29092` and `mongodb://mongodb:27017/`.

---

## Teardown & troubleshooting

```bash
docker compose down            # stop everything
docker compose down -v         # stop and wipe the MongoDB volume
docker compose up --build -d   # rebuild after code changes
```

**No alerts appearing?**
- Give the Flink job ~60–90 s to register, then check
  http://localhost:8081 for a RUNNING job.
- The job reads from the topic's *latest* offset, so it only scores
  transactions produced after it started — the simulator runs continuously, so
  alerts follow shortly.
- Tail the submitter: `docker compose logs flink-job-submitter`.

**Dashboard shows "connecting…"?** The background Kafka tap retries
automatically; once Kafka is healthy it turns 🟢. Confirm with
`docker compose logs -f dashboard`.

**Rebuild a single service:** `docker compose up -d --build dashboard`.
```
