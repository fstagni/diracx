# DX-ADR-012: The DiracX Analytics Platform

## Metadata

- **Created By:** Federico Stagni, Dhiraj Kalita
- **Date:** 2026-11-27
- **Status:** Draft
- **Decision Maker(s):** DiracX development team

## Abstract

DiracX gets an **Analytics Platform**: an OLAP system that collects, stores, and analyses operational data from every domain DiracX handles — workload (jobs, pilots, transformations), data management, storage — as the successor of the DIRAC "Accounting" and "Monitoring" systems, a distinction the platform deliberately does not keep.

The platform follows an **ELT** architecture: data is *extracted* from the OLTP databases, *loaded* into a **Data Lakehouse** (a DuckLake catalog over Parquet files), and *transformed* into time-bucketed aggregates for dashboard consumption. Ingestion is **pull-based** and pluggable: scheduled loaders with incremental queries are the default path, an optional event stream (Kafka API) serves installations with fan-out or multi-source needs, and the historic accounting record retained by DIRAC is imported once into the same sources. Consumption is two-tier: a single `/api/analytics` service with VO-scoped, server-side enforced visibility feeds generic dashboard systems (Grafana via the Infinity plugin), while power users query the lakehouse directly with DuckDB. Jobs and pilots are the first instance of the pattern; storage, data management, and transformation analytics are expected to follow it.

## Motivation

DIRAC's operational data is served today by two ageing, custom systems:

- the **Accounting** system, push-based (clients push to a DataStore), holding aggregated records in MySQL with a custom, hardly maintainable plotting stack;
- the **Monitoring** system (WMSHistory), likewise custom, with an OpenSearch-based rewrite attempt that stalled without community uptake.

Both are replaceable by standard analytics building blocks. The platform's ambition is therefore **not limited to replacing WMSHistory**: it is the successor of DIRAC's entire monitoring and accounting surface, and is expected to grow to every domain DiracX handles — workload (jobs, pilots, transformations/tasks), data management (files, replicas, catalogues), and storage (space usage, transfers) — becoming the single system of record for operational analytics across all of them. Known concrete drivers include accounting plots for MP jobs and wallclock-time accounting, blocked today by the legacy stack.

The distinction between OLAP and OTEL (OpenTelemetry) is kept deliberately: OLAP answers *"how many jobs ran on site X last month?"* (aggregated, historical); OTEL answers *"why did this specific request fail right now?"* (high-cardinality, real-time, system health). They serve different purposes and must not be mixed. An installation can operate without OTEL; it cannot operate meaningfully without an OLAP — hence DuckDB is a core dependency and the platform is not optional.

Further requirements captured with the community:

- VOs must be able to load community-specific data from a DiracX extension.
- It must be possible to load, read, and transform lakehouse data from outside DiracX.
- OLTP sources are not only MySQL (e.g. OpenSearch, Oracle, external APIs).
- The platform must hold, at minimum, **all the data** the legacy DIRAC accounting holds.
- Pre-built dashboard definitions should be persisted in code.
- Building blocks are taken off the shelf wherever possible.
- Concrete reporting obligations (WLCG C-RSG resource reports, LHCC reports) already fix required breakdowns — CPU per site/type/status, storage per tier and data type, weekly or yearly granularity. The catalogue of use cases collected from communities is kept as a living document outside this ADR, so the decision record stays stable as stories evolve.

Legacy systems and the analytics platform coexist during migration: the historic accounting record is imported once into the platform (see *Historic data*), while new data flows in incrementally.

The approach was presented at the 12th Dirac(X) Users' Workshop as *"Analytics in DiracX — A Comprehensive OLAP Approach"* (https://fstagni.github.io/2026_DUW_Analytics_DiracX/).

## Specification

### Terminology

The canonical language is defined in [`GLOSSARY.md`](../../GLOSSARY.md): *analytics platform*, *OLTP source*, *lakehouse*, *ingestion source*, *loader*, *cursor*, *event stream*, *raw*, *bucket*, *bucketing*, *source*, *dimension*, *metric*, *granularity*, *transition*, *occupancy*, *power user*, *VO scope*. ELT — not ETL — is the ingestion pattern: extract from the OLTP, load into the lakehouse, transform for consumption later.

### Architecture

- **One shared lakehouse per installation.** A DuckLake catalog (PostgreSQL recommended; SQLite acceptable for development — it is single-writer) over **Parquet files on S3-compatible object storage**. A POSIX filesystem works but is not advised. DuckDB is not a server: every writer and reader `ATTACH`es the catalog in-process.
- **Raw is the source of truth** and is retained indefinitely. **Buckets** are derived accelerators (hour, day) produced by a bucketing task; missing bucket coverage falls back to on-demand aggregation over raw, so any question is always answerable. Buckets are prunable caches.
- **The lakehouse is the integration point.** Nothing in the serving layer depends on how data arrived.

### Ingestion

The lakehouse does not care *how* data is extracted — only that it arrives. Each source picks its own mechanism; two paths are provided:

1. **Loaders (default, zero new infrastructure).** Scheduled DiracX tasks that extract from one OLTP source with incremental queries (`WHERE LastUpdateTime > ?`, or `WHERE JournalID > ?` for DX-ADR-009 journalled counters, which are a ready-made CDC source), reconstruct domain events, and commit the new events, their aggregates, and their **cursor** to the lakehouse **in one transaction** — cursor and data advance atomically.
2. **Event stream (optional).** A Kafka-API log (e.g. Redpanda) between producers and the lakehouse, for installations that need fan-out to non-analytics consumers or multiple external ingestion sources contributing to the same lakehouse. Consumers commit stream offsets with the data they produce. Shipped upstream behind an extra (`aiokafka`); the producer/aggregator pattern exists in the Belle II extension and can be charted in `diracx-charts` when needed.

Extensions can register additional ingestion sources; loading and reading the lakehouse from outside DiracX is supported by design (it is a DuckLake: any DuckDB client with catalog and storage credentials).

### Historic data

The legacy AccountingSystem retains its **raw accounting records** (the per-type `type_<type>` tables) for the whole history, so the historic record is salvageable at **full fidelity** — no coarser granularity needs to be accepted for old data. (Types may optionally define a `dataTimespan` after which raw records are pruned; where that happened, the legacy adaptive-length bucket tables are the fallback.)

The salvage is a **one-off bulk import**, not a recurring loader:

- Legacy raw records are moved into the corresponding sources (`jobs.accounting`, pilot records into the pilot source), stamped `origin='legacy'` — a provenance dimension every source carries (`live` for loader-collected rows).
- Imported rows have the same schema as live rows. They carry the legacy dimension set; the enriched dimensions (tier, federation, production metadata) are Unknown on them, except where the production metadata still resolves — an implementation choice, per dimension.
- If the import runs while the legacy system still operates, it is extended up to a cutover instant T₀; from T₀ on, only live loaders write (see Open Issues for the seam treatment).
- Queries spanning T₀ sum both strata transparently; grouping by an enriched dimension shows the historic part as Unknown — the honest picture of "full history, richer breakdowns from the analytics era onwards".

The new data is thus an **enrichment on top of** the imported history — per-record grain and richer dimensions going forward — while the historic record itself is never rewritten.

### Serving

- **One service, `/api/analytics`** (not one service per domain). Queryable **sources** are namespaced by domain: `jobs.transitions`, `jobs.time_in_status`, `jobs.counts`, `jobs.events`, `jobs.durations`, `jobs.states`, `jobs.pilots` as the first instances; later e.g. `storage.space`, `dm.replicas`.
- Endpoints: `GET /dimensions` (catalogue of sources with their time column, dimensions, and metrics) and `POST /query` (group/aggregate one source by granularity, with dimension filters — including exclusions — over a time range).
- **Granularities** are minute, hour, day, week, month, and all. Pre-aggregated buckets exist at the finest levels (hour, day); weeks and months are bucketed from daily buckets, which always nest into them. Anything the buckets do not cover falls back to on-demand aggregation over raw.
- **Dimensions may be enriched during transform**: they are not restricted to OLTP columns. Site classifications (tier, federation, country) are derived from site metadata, and production metadata (simulation type, data-taking year, working group, event type, collision type, passage/sprucing version) is joined onto job records at extract time. The vocabulary of these dimensions is owned by the configuration.
- **Levels come in two patterns**: occupancy reconstructed from transitions (job and pilot states), and snapshots (sampled measurements such as storage space used). Sources declare which one they are.
- Each request runs its own in-process DuckDB, memory-bounded and concurrency-capped; responses are JSON (Arrow IPC streams are a possible evolution for heavy consumers).
- **First instance — workload:**
    - *Job status sources* (`jobs.transitions`, `jobs.time_in_status`, `jobs.counts`, `jobs.events`, `jobs.durations`, `jobs.states`, `jobs.pilots`): transitions reconstructed from `JobLoggingDB` history (keyed by the real `(JobID, SeqNum)` primary key); VO read per row from `Jobs.vo` / `PilotAgents.vo` (DiracX installations are multi-VO by default, single-VO being the degenerate case); pilot occupancy sampled per site/CE/status.
    - *Jobs accounting source* (`jobs.accounting`): the superset of legacy DIRAC accounting records — per-job normalised CPU, wall clock, events generated, data volumes, sandbox sizes, final major/minor status — with the production-metadata dimensions above and interval-aware bucketing (jobs spread proportionally across the buckets they overlap, which yields average parallel core usage). This is the source that carries the C-RSG/LHCC CPU reporting.
    - **Core counts** (numberOfProcessors) are a dimension of the job and pilot sources, enabling the MP-job breakdowns and core-based metrics (occupied cores, core-seconds, core-normalised CPU). The OLTP prerequisite — new `JobAttributes` (JobDB) and `PilotAttributes` (PilotAgentsDB) tables, values also saved as JobParameters (DIRACGrid/diracx#294) — does not exist yet; until it does, extraction parses the JDL.
    - *Site capacity* (`sites.capacity`): usable cores per site/CE, sampled from the configuration (CS) — the denominator for occupied-vs-idle plots.
- **Declared next instance — storage:** snapshot sources of space usage per (dataset, replica, storage element) from the replica catalog and SE accounting, with tape/disk as an SE-type dimension and dataset/replica instance counts as distinct-count metrics; popularity as its own source; externally fed endpoints (e.g. EOS non-grid space) through the outside-DiracX ingestion path.

### Access control — VO scope

Visibility is a ladder, enforced **server-side** by injecting filters, never by the dashboard configuration:

| Community role                                 | Sees                                                |
| ---------------------------------------------- | --------------------------------------------------- |
| Community user                                 | only their own activities (`owner` filter injected) |
| Community operator / VO administrator          | everything of their VO                              |
| DiracX administrator (`SERVICE_ADMINISTRATOR`) | everything, across VOs                              |

Sources carry a `vo` dimension; per-user scoping applies wherever an owner dimension makes sense (pilot analytics are VO-scoped). Extensions may override the policy through the `diracx.access_policies` entry point.

### Visualisation

- **Generic users:** pre-built Grafana dashboards using the **Infinity plugin** against `/api/analytics`, with OAuth token passthrough; VO and owner filters are injected by the DiracX API. Users need only a browser.
- **Power users:** direct DuckDB access to the lakehouse (`ATTACH` + notebooks, CLI, or `diracx-mcp`), per-VO or per-installation, without DiracX running. Direct access is governed by storage/catalog credentials (short-lived credentials may be minted by the API).

### Deployment

DiracX runs on Kubernetes via `diracx-charts`. The loader path adds **zero new services**: loaders run in the existing task workers, the catalog is the existing PostgreSQL, the data lives in the already-required S3. Only the optional event-stream path introduces a new stateful service (Redpanda or alike).

## Rationale

- **A lakehouse, not a warehouse or a lake:** raw flexibility with governed, query-optimised layers; DIRAC's legacy accounting "resembles" a warehouse, and the platform preserves its proven bucketing model while making raw data first-class.
- **DuckDB + DuckLake:** in-process (no server to deploy), ACID transactions over multi-table commits (which is what makes loader cursors atomic), snapshots/time travel, schema evolution, open Parquet storage, and a "multiplayer" concurrency model vanilla DuckDB lacks. Each reader — API request, power-user laptop — brings its own compute.
- **Pull-based extraction:** mirrors how the data already lives (indexed `LastUpdateTime` columns, journalled counters) and removes the failure-prone push machinery of the legacy accounting clients.
- **Incremental queries over log-based CDC:** no coupling to a specific OLTP implementation, no milli-second-freshness requirement, no triggers or binlog parsing to maintain.
- **Dual ingestion paths:** the loader keeps every installation able to run analytics with nothing new to operate; the event stream exists because fan-out and multi-source installations are real, but it is optional so that simplicity remains the default.
- **One shared lakehouse:** cross-domain correlation (jobs with storage, transformations with data movement) is the point of a lakehouse; domain isolation happens at the table level.
- **One analytics service:** the source/dimension/metric query model is domain-agnostic; per-domain services would multiply near-identical routers and foreclose cross-domain queries at the API level.
- **The visibility ladder:** analytics includes per-user detail (owner is a dimension), so server-side filter injection — not dashboard configuration — is what makes the ladder trustworthy; power users are excluded because their direct access is credential-governed.

## Rejected Ideas

- **Developing our own solution:** the market offers standard building blocks; the legacy systems are the cautionary tale.
- **Continuing the OpenSearch-based approach:** issues were opened, no community uptake followed.
- **ClickHouse** as the OLAP engine: it would be yet another service to deploy and operate.
- **Log-based CDC** (MySQL binlog, Oracle redo log): coupled to the OLTP technology, perceived as an added complication, and milli-second freshness is not needed.
- **Grafana DuckDB plugin** (direct DB dashboards): not officially supported by Grafana, requires specific images, scalability concerns.
- **Kafka as the mandatory ingestion buffer** (the initial Belle II design): it would force a stateful service on every installation and, since events are reconstructed from full source history anyway, its ordering guarantees are redundant for correctness — while cursor-and-data-in-one-transaction loaders are strictly safer than publish-then-commit. Kept only as the optional event-stream path; a future ADR may supersede this one if fan-out demand materialises.
- **ETL** (transform before load): raw is the source of truth; transformation happens in the lakehouse, after load.
- **Importing legacy bucket aggregates** for the historic record: the raw accounting records were never pruned, so full-fidelity history is available; bucket rows would only add coarseness. Bucket tables remain the fallback for a type that did prune its raw records.
- **A recurring historic-import loader**: the salvage is a one-off import; recurring machinery would duplicate the live loaders' contract for no benefit.

## Open Issues

- Bucket sizes and retention windows (hourly/daily/monthly) — open to community feedback.
- File-type / data-quality vocabulary of the storage source (RAW, DST, MC-DST, USER, BUFFER, ...) — the breakdown LHCb itself flags as undecided.
- Unit normalisation: CPU stored as normalised seconds, with HS23 (and future recalibrations) applied as constants at query/dashboard level — confirm with C-RSG reporting practice.
- Historic-import scope: legacy AccountingSystem first; whether the legacy monitoring record (WMSHistory in OpenSearch) is backfilled the same way into the `jobs.states` historic stratum.
- The T₀ seam: whether the "import up to T₀, loaders from T₀" contract needs a stricter dedup rule, or the `origin` dimension as audit trail suffices.
- Whether enriched dimensions (tier, federation, production metadata) are backfilled onto imported historic rows where the production metadata still resolves.
- Response format evolution (JSON today; Arrow IPC streams for heavy consumers?).
- Mechanism for designating community operator/VO-administrator roles (per-VO property vs group semantics).
- Short-lived credential minting for power-user direct access.
- Which domain is implemented next after jobs/pilots (storage vs data management vs transformations).
