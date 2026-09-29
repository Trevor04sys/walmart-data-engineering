 Stage 6 — Spark Optimization

 Overview

This stage investigates how Spark and Databricks execute queries against the project's Delta tables. The goal is to understand physical plans and use Query Profile metrics to identify what the engine actually did.

Experiments cover column pruning, predicate pushdown, join strategy selection, aggregation and shuffle-plan inspection, table layout, small files, runtime profiling, combined filtering and projection, and the current `obt_b` join plan.

Measurement principle: conclusions distinguish what was directly visible in an execution plan or Query Profile from what was not measured. A plan alone does not prove a runtime speedup, and a single Query Profile run is not a controlled benchmark.

 Project context

The main table examined is:

`walmart_catalog.silver_business.obt_b`

The current OBT is intended to have one row per `order_item_id`. It is built from:

- `orders_t` (base)
- `customers_t`
- `order_items_t`
- `products_t`
- `stores_t`

The current model uses four `LEFT JOIN`s and does not join `employees_t`.

Previously validated OBT results:

- 30,021 total rows
- 30,021 distinct `order_item_id` values
- 10,000 distinct orders

These are data-validation results, not performance measurements.

---

 1. Column pruning

 Purpose

Column pruning means Spark reads only the columns required by a query rather than every column in a table.

 Experiments

Full projection:

```sql
EXPLAIN FORMATTED
SELECT *
FROM walmart_catalog.silver_business.obt_b;
```

Limited projection:

```sql
EXPLAIN FORMATTED
SELECT
    order_item_id,
    order_id,
    customer_id,
    product_id,
    store_id,
    quantity,
    line_amount
FROM walmart_catalog.silver_business.obt_b;
```

 Observations

- The `SELECT *` plan showed a `PhotonScan` reading all 48 columns.
- The projected query's scan read only the seven requested columns.
- The projected plan included `PhotonProject`, `PhotonColumnarToRow`, and `PhotonResultStage`.

 Conclusion

Column pruning was confirmed in the physical plan: the projected query required fewer columns at the scan. Runtime observations were evaluated separately in the Query Profile section.

---

 2. Predicate pushdown

 Purpose

Predicate pushdown means a filter can be applied close to the data scan.

 Experiment

```sql
EXPLAIN FORMATTED
SELECT
    order_item_id,
    order_id,
    customer_id,
    product_id,
    store_id,
    quantity,
    line_amount
FROM walmart_catalog.silver_business.obt_b
WHERE order_timestamp >= '2026-06-01';
```

 Observations

- The plan showed the date predicate in `DictionaryFilters` and `RequiredDataFilters` on the scan.
- The scan read the selected output columns plus `order_timestamp`, which was needed to evaluate the filter.

 Conclusion

The plan confirmed that the date filter was pushed down to the scan. This does not by itself prove that files or bytes were skipped; file skipping and runtime impact require separate evidence.

---

 3. Join optimization

 Purpose

Inspect the physical join strategy selected by the optimizer and compare it with explicitly requested strategies.

 Observations

For the actual `obt_b`-to-`customers_t` inner join:

- The optimizer selected `PhotonBroadcastHashJoin Inner`.
- The OBT scan read three columns and the customer scan read two columns.
- The customer side used a single-partition broadcast preparation exchange.

An explicit `BROADCAST(c)` hint produced essentially the same plan.

An explicit `MERGE(c)` hint produced a `SortMergeJoin`, hash-partitioning exchanges on both sides (16 partitions), and sorts. The plan was not fully Photon-supported.

 Conclusion

For this join, the optimizer naturally selected a broadcast hash join. Forcing a merge join introduced exchanges and sorts in the inspected plan.

No controlled runtime benchmark was performed, so this is a physical-plan comparison—not proof of a measured performance difference. No join hint was added to the model.

---

 4. Aggregation and shuffle-plan inspection

 Purpose

Inspect the physical plan for a grouped aggregation and check which exchanges and aggregation operators are visible.

 Experiment

```sql
EXPLAIN FORMATTED
SELECT
    customer_id,
    COUNT(*) AS order_item_count,
    SUM(line_amount) AS total_sales
FROM walmart_catalog.silver_business.obt_b
GROUP BY customer_id;
```

 Observations

The inspected plan included:

- `AdaptiveSparkPlan`
- `PhotonResultStage`
- `PhotonColumnarToRow`
- `PhotonGroupingAgg`
- `PhotonScan`

The scan read `customer_id` and `line_amount`. No explicit `Exchange` or separate partial/final aggregation was shown in the displayed plan. The plan was fully Photon-supported.

 Conclusion

The displayed plan shows a Photon grouping aggregation and no explicit exchange or separate partial/final aggregation operators. This conclusion is limited to the operators visible in this plan; it does not establish that other query shapes or execution conditions will never require a shuffle.

Metric meaning: `COUNT(*)` counts OBT rows for each customer. Given the intended OBT grain, this is an order-item-row count, not a distinct-order count.

---

 5. Table layout and data skipping

 Purpose

Inspect the Delta table's physical layout and determine what the query plan reveals about filtering.

 Table detail

Command:

```sql
DESCRIBE DETAIL walmart_catalog.silver_business.obt_b;
```

Observed properties:

| Property | Observed value |
| --- | --- |
| Format | Delta |
| Partition columns | `[]` |
| Clustering columns | `[]` |
| Number of files | `1` |
| Size | `1,777,519` bytes (about 1.69 MiB) |
| Deletion vectors enabled | Yes |
| Deletion vectors / deleted rows observed | None |

 Date-filter plan

A date-filtered query was inspected with `EXPLAIN FORMATTED`. The plan showed a `PhotonScan` reading the selected columns and date column, with the date conditions in `DictionaryFilters` and `RequiredDataFilters`.

There was no partition-pruning opportunity because the table had no partition columns. The plan did not prove that a file was skipped.

 Conclusion

The table was not partitioned or clustered. The filter was pushed to the scan, but the plan did not prove file skipping. No data-layout change was made.

---

 6. Small-file inspection

 Purpose

Check whether the OBT has many small files that might justify compaction.

 Observations

A repeat `DESCRIBE DETAIL` reported the same one data file and 1,777,519 bytes total size.

 Conclusion

The inspected table did not show a small-file problem: it consisted of one file of about 1.69 MiB. No `OPTIMIZE`, repartitioning, or compaction change was made just for this demonstration. This conclusion applies to the table state inspected during the experiment.

---

 7. Runtime profiling: full versus projected query

 Purpose

Use Databricks Query Profile to compare observed scan metrics for a full-row query and a projected query.

 Initial profile observations

| Metric | Full `SELECT *` profile | Projected query profile |
| --- | ---: | ---: |
| Rows returned | 2,693 | 10,000 |
| Rows read | 4,096 | 12,288 |
| Bytes read | 1.83 MB | 521.34 KB |
| Files | 1 | 1 |
| Files pruned | 0 | 0 |
| Cache | 20% | 100% |
| Wall-clock time | about 2.065 s | about 1.223 s |
| Execution time | about 1.412 s | Not recorded here |
| Optimization / file-pruning time | about 653 ms | Not recorded here |
| Task time | about 684 ms | Not recorded here |

These initial profiles did not use a matching row limit, and their returned-row counts differed. They are not a like-for-like benchmark. A separate `COUNT(*)` query returned 30,021 rows for `obt_b`; the reason the earlier profiles returned fewer rows was not established.

 Equal-limit comparison

Full-row query:

```sql
SELECT *
FROM walmart_catalog.silver_business.obt_b
LIMIT 1000;
```

Projected query:

```sql
SELECT
    order_item_id,
    order_id,
    customer_id,
    product_id,
    store_id,
    quantity,
    line_amount
FROM walmart_catalog.silver_business.obt_b
LIMIT 1000;
```

 Observed Query Profile metrics

| Metric | `SELECT * LIMIT 1000` | Seven-column query `LIMIT 1000` |
| --- | ---: | ---: |
| Rows returned | 1,000 | 1,000 |
| Rows read | 4,096 | 4,096 |
| Bytes read | 1.81 MB | 521.34 KB |
| Files | 1 | 1 |
| Files pruned | 0 | 0 |
| Cache | 100% | 100% |
| Task time | 37 ms | 11 ms |
| Spill | None | None |

 Interpretation

In these individual runs, both queries returned 1,000 rows, read 4,096 rows from one file, reported 100% cache and zero files pruned, and reported no spill. The projected query reported 521.34 KB read versus 1.81 MB for `SELECT *`; task time was 11 ms versus 37 ms.

The lower reported bytes are consistent with the column-pruning behavior seen in the physical plan.

 Limitations

These are individual Query Profile observations, not a controlled benchmark. Cache state, execution variability, and other runtime factors can affect timings. The task-time difference should not be generalized as a guaranteed speedup.

---

 8. Combined column pruning and predicate pushdown

 Purpose

Inspect a query that combines a limited projection with a bounded date filter.

 Experiment

```sql
EXPLAIN FORMATTED
SELECT
    order_id,
    customer_id,
    product_id,
    store_id,
    quantity,
    line_amount
FROM walmart_catalog.silver_business.obt_b
WHERE order_timestamp >= '2026-06-01'
  AND order_timestamp < '2026-07-01';
```

 Observations

- `PhotonScan` read the six selected output columns plus `order_timestamp` for filtering.
- The date predicates appeared in `DictionaryFilters` and `RequiredDataFilters`.
- `PhotonProject` produced the six requested output columns.
- The plan was fully Photon-supported.

 Conclusion

The plan demonstrated both column pruning and predicate pushdown in the same query. No runtime profile was recorded for this combined query. Since the table was unpartitioned, partition pruning did not apply; the plan also did not establish that a file was skipped.

---

 9. Current `obt_b` join plan

 Purpose

Inspect the physical plan for the current compiled OBT SQL, ensuring the plan corresponds to the current model definition.

 Current model shape

The current compiled SQL uses `orders_t` as the base and joins `customers_t`, `order_items_t`, `products_t`, and `stores_t`. All four joins are `LEFT JOIN`s. The current model does not join `employees_t`.

 Observations

The `EXPLAIN FORMATTED` plan showed:

- Four `PhotonBroadcastHashJoin LeftOuter` operators
- Single-partition broadcast preparation exchanges for the right-side tables
- No visible hash-partitioning exchanges
- No `SortMergeJoin`
- Full optimizer statistics for all five scanned tables
- 48 output columns
- Full Photon support
- `AdaptiveSparkPlan isFinalPlan=false`

 Conclusion

For the current OBT query, the optimizer selected broadcast hash joins for all four joins in the inspected initial physical plan. No sort-merge joins or visible hash-partitioning exchanges appeared.

The plan is marked `isFinalPlan=false`, so it is the initial adaptive plan, not a final adaptive plan. No runtime benchmark was performed for this full OBT query. No hints or model changes were made based on this plan.

---

 Summary of findings

| Area | Established | Not established |
| --- | --- | --- |
| Column pruning | The scan read only requested columns for the projected query. | A universal runtime improvement. |
| Predicate pushdown | Date filters appeared in scan-level filter fields. | File skipping or bytes saved from the plan alone. |
| Join optimization | Broadcast hash joins were selected in inspected joins; a forced merge plan added exchanges and sorts. | A controlled runtime winner between strategies. |
| Aggregation | The displayed plan used `PhotonGroupingAgg`; no explicit exchange was visible. | That all aggregation queries will avoid shuffles. |
| Table layout | The table had no partitioning or clustering. | Benefits from partitioning or clustering. |
| Small files | The inspected table had one file of about 1.69 MiB. | That other tables or future table states have no small-file issues. |
| Runtime profile | Under `LIMIT 1000`, the projected query reported fewer bytes and lower task time in the observed runs. | A repeatable or guaranteed speedup. |
| Combined filtering and projection | The plan showed both pruning and scan-level filtering. | Runtime impact for the combined query. |
| Full OBT plan | Four broadcast hash joins were selected in the initial adaptive plan. | Final adaptive behavior or runtime performance. |

 Changes made

No query hints, repartitioning, clustering, partitioning, or compaction changes were made as a result of these experiments.

This was intentional: the stage focused on inspecting and understanding execution behavior, and the available evidence did not justify changing the model or table layout.

 Final takeaway

The experiments confirmed that Databricks' optimizer applied column pruning and scan-level filtering for the tested queries, and selected broadcast hash joins for the inspected joins. Query Profile showed lower bytes read and task time for the projected query in the specific `LIMIT 1000` runs.

The OBT was small and stored in one file at the time of inspection, so the experiments did not demonstrate a need for partitioning, clustering, or compaction. Runtime observations are documented as observations rather than broad performance claims.

The main outcome of Stage 6 is a documented understanding of the physical plans and the limits of the measurements—not a set of speculative performance changes.
