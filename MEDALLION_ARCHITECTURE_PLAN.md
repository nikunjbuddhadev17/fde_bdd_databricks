# Medallion Architecture Implementation Plan
## Theater Management FDE Interview Packet

**Prepared:** 2026-11-09  
**Objective:** Implement medallion architecture (Bronze → Silver → Gold layers) to address data quality and support operational decision-making.

---

## 1. Executive Summary

The current as-is architecture ingests raw data from three sources and immediately joins them into a denormalized table that powers the dashboard. This approach has critical limitations:

| Issue | Impact | Medallion Solution |
|-------|--------|-------------------|
| **Silent data loss** | INNER JOINs drop orphan rows; managers can't see what was lost | Bronze keeps raw data; Silver quarantines invalid records for review |
| **No data lineage** | Can't trace which records failed and why | Each layer maintains quality metadata; audit trail preserved |
| **Unmaintainable transforms** | All logic in one denormalization query | Separate concerns: cleanse (Silver), enrich (Gold), aggregate (Analytics) |
| **No way to handle duplicates** | POS systems (esp. kiosk_v1_batch) produce 4.5% duplicate rate | Silver deduplicates with deterministic handling rules |
| **Out-of-order events** | No support for late-arriving data or corrections | Silver applies event ordering; Gold handles slowly-changing dimensions |

**The medallion architecture separates concerns:**
- **Bronze:** Raw data, as-is, immutable (source of truth)
- **Silver:** Cleaned, validated, deduplicated (business-ready for reuse)
- **Gold:** Aggregated, enriched, business context (optimized for analytics)

---

## 2. Target State Architecture

### 2.1 Catalog Structure

```
catalog: theater_management
├── bronze (ingestion layer)
│   ├── pos_ticket_sales (raw)
│   ├── theater_layout (raw)
│   └── imdb_movie_metadata (raw)
├── silver (cleansing & validation layer)
│   ├── stg_pos_ticket_sales (deduplicated, validated, ordered)
│   ├── stg_theater_layout (deduplicated, validated)
│   ├── stg_imdb_movie_metadata (deduplicated, validated)
│   └── pos_quality_issues (rejected/quarantined records)
└── gold (business/analytics layer)
    ├── fct_ticket_sales (denormalized, business-ready)
    ├── dim_theater (SCD Type 2 - slowly changing dimensions)
    ├── dim_movie (SCD Type 2 - slowly changing dimensions)
    └── agg_daily_sales_by_theater (pre-aggregated KPIs) [future]
```

### 2.2 Data Flow

```
CURRENT (as-is):
pos_ticket_sales (raw)  ────────┐
theater_layout (raw)    ────────┤→ [INNER JOIN] → denormalized_theater_sales → Dashboard
imdb_movie_metadata (raw) ─────┘


TARGET (medallion):
pos_ticket_sales (raw)     →  Silver Layer (Cleanse)    →  Gold Layer (Enrich)    → Dashboard
  (POS systems all mixed)      - Dedup by txn_id               - Join dimensions        - Pre-agg views
                                - Handle nulls & invalid        - Add business logic     - Slow-changing dims
                                - Validate FK references        - Create fact/dim tables - ML features
                                - Order events
                                
theater_layout (raw)       →  Silver Layer              →  Gold Layer
imdb_movie_metadata (raw)  →  Silver Layer              →  Gold Layer
                           
                           Quality Issues Captured → Reporting/Alerting
```

---

## 3. Silver Layer Detailed Design

### 3.1 Silver Transformations for POS Ticket Sales

**File:** `src/transform/silver_pos_ticket_sales.py`

#### Data Quality Issues to Handle (from DQ profiles):

| Issue | Source(s) | Rate | Handling Strategy |
|-------|-----------|------|------------------|
| **Duplicate transactions** | All (0.5%-4.5%) | `duplicate_txn_rate` | Keep earliest by `purchase_timestamp`, mark duplicates in quality table |
| **Null screen_id** | pos_v2_batch (0.5%), kiosk_v1_batch (4%) | `null_screen_rate` | Reject record; quarantine with reason |
| **Null showtime** | pos_v2_batch (2%), kiosk_v1_batch (3%) | `null_showtime_rate` | Reject; quarantine |
| **Invalid ticket_price** | pos_v2_batch (0.5%), kiosk_v1_batch (4%) | `bad_price_rate` | Reject `price <= 0` or `price > 100`; quarantine |
| **Malformed purchase_timestamp** | pos_v2_batch (3%), kiosk_v1_batch (2.5%) | `bad_timestamp_rate` | Attempt parse (legacy "%m/%d/%Y %I:%M %p"); reject if unparseable |
| **Orphan theater_id** | kiosk_v1_batch (6%) | `orphan_theater_rate` | LEFT JOIN theater_layout; quarantine nulls |
| **Orphan movie_id** | All (0.1%-4%) | `orphan_movie_rate` | LEFT JOIN imdb_metadata; quarantine nulls |
| **Format mismatch** | All (0.1%-3%) | `format_mismatch_rate` | Invalid format on IMAX-capable screens; quarantine |
| **Out-of-order events** | POS batch systems | implicit | Sort by `theater_id`, `screen_id`, `purchase_timestamp` before deduplication |

#### Processing Steps:

1. **Window cleanse** (parse timestamps, coerce types)
   - Accept legacy timestamp format from pos_v2_batch
   - Coerce string nulls to SQL NULL
   - Flag rows that fail parsing

2. **Deduplication** (deterministic tie-breaking)
   - Partition by `transaction_id`
   - Keep earliest `purchase_timestamp`
   - Mark duplicates for reporting

3. **Referential integrity** (FK validation)
   - LEFT JOIN `bronze.theater_layout` on `(theater_id, screen_id)`
   - LEFT JOIN `bronze.imdb_movie_metadata` on `movie_id`
   - Quarantine rows with null foreign keys

4. **Business rule validation**
   - `ticket_price` must be > 0 and ≤ 100
   - `seats_sold` must be > 0
   - If `format = 'IMAX'`, screen must be `imax_capable = true`

5. **Ordering** (for out-of-order events)
   - Sort by `theater_id`, `screen_id`, `showtime`, `purchase_timestamp`
   - Enables correct replay and idempotency

#### Output Tables:

- **`silver.stg_pos_ticket_sales`** — Clean, valid, deduplicated records ready for join
- **`silver.pos_quality_issues`** — Rejected records with rejection reason, source POS system, timestamp

#### Audit Columns (added in Silver):
```python
data_quality_flag: BOOLEAN  # true if any validation failed, quarantined
rejection_reason: STRING    # "duplicate", "null_screen_id", "invalid_price", "orphan_theater", etc.
quality_checked_at: TIMESTAMP  # when the record was validated
ingest_timestamp: TIMESTAMP  # when record arrived in Bronze
```

---

### 3.2 Silver Transformations for Theater Layout

**File:** `src/transform/silver_theater_layout.py`

#### Transformations:

1. **Deduplication** by `screen_id` (natural key)
   - Unlikely but handle if multiple rows exist
   - Keep latest by `_ingested_timestamp`

2. **Null validation**
   - All columns required non-null
   - Quarantine any nulls

3. **Business rule validation**
   - `open_time` < `close_time`
   - `row_count` > 0, `seats_per_row` > 0
   - `screen_type` in approved list

#### Output Tables:
- **`silver.stg_theater_layout`** — Clean layout data
- **`silver.theater_layout_quality_issues`** — Rejected records

---

### 3.3 Silver Transformations for IMDB Movie Metadata

**File:** `src/transform/silver_imdb_movie_metadata.py`

#### Transformations:

1. **Deduplication** by `imdb_id` (natural key)
   - Full refresh from IMDB; keep latest only

2. **Null validation**
   - Critical fields: `title`, `imdb_id`, `release_date`

3. **Data type coercion**
   - `runtime_minutes` must be integer > 0
   - `imdb_rating` must be 0.0-10.0
   - `release_date` must be valid date

#### Output Tables:
- **`silver.stg_imdb_movie_metadata`** — Clean metadata
- **`silver.imdb_quality_issues`** — Rejected records

---

## 4. Bronze Layer

**No changes required** — Bronze remains the immutable source of truth.

**Current state (to preserve):**
- `bronze.pos_ticket_sales` — Raw POS data as-is
- `bronze.theater_layout` — Raw theater data as-is
- `bronze.imdb_movie_metadata` — Raw movie metadata as-is

**Important:** All three are full-refreshed on each seed run; use `WRITE_TRUNCATE` mode to ensure idempotency.

---

## 5. Implementation Phases

### Phase 1: Infrastructure & Catalog Setup (Day 1)
- [ ] Create `theater_management` catalog (if not exists)
- [ ] Create three schemas: `bronze`, `silver`, `gold`
- [ ] Update `databricks.yml` variables:
  - Change default `catalog` from "workspace" to "theater_management"
- [ ] No data changes yet; seed jobs still point to old `workspace.fde_theater` for now

### Phase 2: Consolidated Medallion Pipeline Job (Day 2-3)
Single job: `build_medallion_theater_sales` with multiple sequential tasks:

**Task 1: `seed_theater_data`** (existing)
  - Generates and writes bronze tables: pos_ticket_sales, theater_layout, imdb_movie_metadata
  - No changes to this task

**Task 2: `silver_pos_ticket_sales`** (depends on Task 1)
  - Input: Bronze tables
  - Output: `silver.stg_pos_ticket_sales`, `silver.pos_quality_issues`
  - Metrics: rows input, rows accepted, rows rejected by category

**Task 3: `silver_theater_layout`** (depends on Task 1)
  - Input: Bronze tables
  - Output: `silver.stg_theater_layout`, `silver.theater_layout_quality_issues`

**Task 4: `silver_imdb_movie_metadata`** (depends on Task 1)
  - Input: Bronze tables
  - Output: `silver.stg_imdb_movie_metadata`, `silver.imdb_quality_issues`

**Task 5: `gold_fct_ticket_sales`** (depends on Tasks 2, 3, 4)
  - Input: Silver tables + Bronze (for latest dimension values)
  - Output: `gold.fct_ticket_sales`
  - Join logic: LEFT JOIN (preserve all Silver rows)

**Task 6: `quality_summary`** (depends on Task 5)
  - Generate quality metrics and logging
  - Report: rows input → silver → gold, rejection rates by POS system

### Phase 3: Quality Monitoring & Reporting (Day 4)
- [ ] Create quality dashboard showing:
  - Rows accepted/rejected per POS system
  - Top rejection reasons by volume
  - Trend over time
  - Data freshness
  
- [ ] Create alerting rules:
  - Alert if rejection rate exceeds threshold for any POS system
  - Alert if orphan theater/movie rate spikes

### Phase 4: Cutover & Decommission (Day 5)
- [ ] Verify Gold layer matches current denormalized behavior (within margin)
- [ ] Run side-by-side validation
- [ ] Decommission old `denormalized_theater_sales` table
- [ ] Update job dependencies

---

## 6. Key Design Decisions

### 6.1 Deduplication Strategy

**Question:** When a duplicate `transaction_id` exists, which row wins?

**Decision:** **Keep earliest by `purchase_timestamp`**
- Reasoning: If a row is replayed later, the original purchase time is truth
- Implementation:
  ```python
  window_spec = Window.partitionBy("transaction_id").orderBy("purchase_timestamp")
  df = df.withColumn("rn", row_number().over(window_spec))
  df_dedup = df.filter(col("rn") == 1)
  df_dupes = df.filter(col("rn") > 1)  # Quarantine these
  ```

### 6.2 Null Handling

**Question:** Should Silver drop or quarantine records with nulls?

**Decision:** **Quarantine with detailed reason**
- Bronze is immutable; don't lose data
- Silver provides the "cleaned" subset for reporting
- Quarantine table is discoverable for support teams
- Enables post-mortem RCA without re-ingesting

### 6.3 Foreign Key Validation

**Question:** Should Silver use INNER JOIN (current behavior) or LEFT JOIN?

**Decision:** **LEFT JOIN + quarantine nulls**
- Left join allows tracking which POS records have bad references
- Quarantine table surfaces systemic problems (e.g., kiosk_v1_batch has 6% orphan theater refs)
- Current INNER JOIN loses visibility; new approach reveals the problem

### 6.4 Out-of-Order Event Handling

**Question:** How should late-arriving data be handled?

**Decision:** **Silver applies deterministic ordering; Gold handles SCD**
- Silver sorts by (theater_id, screen_id, showtime, purchase_timestamp)
- Ensures idempotent processing and correct replay
- Gold layer (future) can use SCD Type 2 for movie metadata changes

---

## 7. Success Criteria

### 7.1 Functional Requirements
- [ ] Bronze: All source data persists unchanged
- [ ] Silver: 
  - Duplicates identified and quarantined
  - Data quality issues tracked with reason codes
  - All cleanup is declarative (not hard-coded thresholds)
  - Idempotent: Same input → Same output (deterministic tie-breaking)
- [ ] Gold: 
  - Row counts match or exceed current denormalized table (LEFT JOIN)
  - Dashboard KPIs stable (same numbers as before)
  - Quality metadata visible to downstream

### 7.2 Operational Requirements
- [ ] All three layers support full-refresh and incremental (future)
- [ ] Job execution time reasonable (~5-10 min total for full pipeline)
- [ ] Quality tables indexed for fast root-cause analysis
- [ ] Audit trail preserved (timestamps, source tracking)

### 7.3 Data Governance
- [ ] PII handling (if any) documented
- [ ] Data retention policy clear (when to purge quality tables)
- [ ] Ownership assigned per schema

---

## 8. Testing Strategy

### 8.1 Unit Tests (per transformation module)
- Silver POS: Verify deduplication, FK validation, price range checks
- Silver Layout: Verify null handling, time range validation
- Silver IMDB: Verify rating range, date format

### 8.2 Integration Tests
- Row counts: `bronze → silver → gold` flow
- Quality issues: Known bad records correctly quarantined
- Side-by-side: Gold fact table = current denormalized (or better)

### 8.3 Data Quality Assertions
- No duplicates in Silver output (checked via `count(distinct transaction_id)`)
- All rejected records in quality tables
- FK references in Gold are all non-null (after LEFT JOIN)

---

## 9. Future Enhancements (Out of Scope for MVP)

1. **Incremental loading** — Currently full-refresh; add CDC support
2. **Slowly Changing Dimensions** — SCD Type 2 for theater and movie updates
3. **Data lineage tracking** — OpenLineage integration
4. **Quality metrics dashboards** — Real-time DQ monitoring per POS system
5. **Materialized aggregates** — Pre-computed daily/weekly sales KPIs in Gold
6. **Predictive analytics** — Features for ML models built on Gold layer
7. **Cross-system reconciliation** — Compare POS systems' reported vs. actual revenue

---

## 10. Implementation Checklist

### Code Changes
- [ ] Create `src/transform/silver_pos_ticket_sales.py`
- [ ] Create `src/transform/silver_theater_layout.py`
- [ ] Create `src/transform/silver_imdb_movie_metadata.py`
- [ ] Create `src/transform/gold_fct_ticket_sales.py`
- [ ] Update `resources/jobs.yml` with new job definitions
- [ ] Update `databricks.yml` catalog/schema variables
- [ ] Update or create `src/dashboard/theater_performance_gold.lvdash.json`

### Testing
- [ ] Unit tests for each Silver module
- [ ] Integration test: Full pipeline run
- [ ] Validation: Gold output vs. current denormalized

### Documentation
- [ ] Schema documentation (column lineage, business definitions)
- [ ] Data dictionary for quality issue codes
- [ ] Runbook for responding to high rejection rates

### Deployment
- [ ] Stage in dev workspace
- [ ] Run full seed + medallion pipeline
- [ ] Validate dashboard
- [ ] Deploy to prod
- [ ] Monitor first 24-48h for alerts

---

## 11. Appendix: Data Quality Issue Categories

When a record is rejected and moved to a quality table, the `rejection_reason` column will contain one of:

| Reason | Table | Handling |
|--------|-------|----------|
| `duplicate_transaction` | pos_quality_issues | Kept earliest; later occurrences quarantined |
| `null_screen_id` | pos_quality_issues | Rejected (critical FK) |
| `null_showtime` | pos_quality_issues | Rejected (critical for business logic) |
| `null_theater_id` | pos_quality_issues | Rejected (critical FK) |
| `invalid_ticket_price` | pos_quality_issues | Rejected if price ≤ 0 or > 100 |
| `invalid_seats_sold` | pos_quality_issues | Rejected if ≤ 0 |
| `malformed_timestamp` | pos_quality_issues | Rejected; unable to parse to TIMESTAMP |
| `orphan_theater` | pos_quality_issues | LEFT JOIN returned null; no match in layout |
| `orphan_movie` | pos_quality_issues | LEFT JOIN returned null; no match in metadata |
| `format_mismatch` | pos_quality_issues | Format='IMAX' but screen.imax_capable=false |
| `duplicate_screen` | theater_layout_quality_issues | Multiple rows with same screen_id |
| `null_required_field` | theater_layout_quality_issues | Null in critical column (theater_id, screen_id, etc.) |
| `invalid_time_range` | theater_layout_quality_issues | open_time >= close_time |
| `invalid_seating` | theater_layout_quality_issues | row_count ≤ 0 or seats_per_row ≤ 0 |
| `duplicate_movie` | imdb_quality_issues | Multiple rows with same imdb_id |
| `invalid_rating` | imdb_quality_issues | imdb_rating not in [0.0, 10.0] |
| `invalid_runtime` | imdb_quality_issues | runtime_minutes ≤ 0 |

---

## Next Steps

1. **Review** this plan with stakeholders
2. **Clarify** any assumptions (e.g., deduplication tie-breaking, retention policy)
3. **Prioritize** phases (can Phase 4 wait, or needed for MVP?)
4. **Assign** development ownership per module
5. **Proceed** to implementation (Phase 1 on Day 1)

---

**Document Status:** Draft - Ready for Review  
**Last Updated:** 2026-11-09
