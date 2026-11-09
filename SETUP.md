# Setup Instructions

This guide walks through deploying the FDE interview packet to a Databricks workspace and initializing the as-is theater sales pipeline and dashboard the candidate will work from.

## Prerequisites

- Databricks CLI >= 0.260.0 (`brew install databricks` or [docs](https://docs.databricks.com/aws/en/dev-tools/cli/install))
  - **HIGHLY RECOMMENDED** If you already have the Databrick CLI installed you should upgrade it.  [docs](https://docs.databricks.com/aws/en/dev-tools/cli/install#homebrew-update-for-linux-or-macos)
- A Databricks workspace (Free Edition or higher)
- A CLI profile configured for the target workspace, e.g. `databricks auth login --host <your-workspace-host> --profile=my-profile`
- A SQL warehouse — the bundle looks up `Serverless Starter Warehouse` by default (present on fresh Free Edition workspaces); override with `--var="warehouse_id=<id>"` if it has been renamed or deleted

## 1. Deploy the bundle

This registers the two jobs, uploads the dashboard definition, and creates the workspace folder under `/Workspace/Users/<you>/fde_interview/`. The catalog and schema are created by the seed job.

```bash
databricks bundle deploy -t dev --profile=my-profile
```

Override catalog/schema if needed:

```bash
databricks bundle deploy -t dev --profile=my-profile \
  --var="catalog=my_catalog" \
  --var="schema=my_schema"
```

## 2. Seed source tables

The `seed_theater_data` job runs three ingest tasks: `ingest_theater_layout` and `ingest_imdb_metadata` in parallel, followed by `ingest_pos_ticket_sales` (which reads the first two for FK-consistent transactions). All data is generated deterministically (`seed=42`).

```bash
databricks bundle run -t dev seed_theater_data --profile=my-profile
```

Verify (adjust catalog/schema to match your vars):

```sql
SHOW TABLES IN <catalog>.<schema>;
-- Expect: theater_layout, imdb_movie_metadata, pos_ticket_sales

SELECT COUNT(*) FROM <catalog>.<schema>.pos_ticket_sales;
-- Expect: roughly 150k–200k transactions
```

## 3. Build the denormalized table

The `build_denormalized_theater_sales` job inner-joins the three source tables into the wide table that powers the dashboard. Run after `seed_theater_data`.

```bash
databricks bundle run -t dev build_denormalized_theater_sales --profile=my-profile
```

Verify (adjust catalog/schema to match your vars):

```sql
SELECT COUNT(*) FROM <catalog>.<schema>.denormalized_theater_sales;
```

## 4. Verify the workspace

After both jobs complete, the workspace should have:

| Asset | Location | How to verify |
|-------|----------|---------------|
| 3 source tables | `<catalog>.<schema>` | `SHOW TABLES` returns `theater_layout`, `imdb_movie_metadata`, `pos_ticket_sales` |
| 1 denormalized table | `<catalog>.<schema>.denormalized_theater_sales` | `DESCRIBE <catalog>.<schema>.denormalized_theater_sales` |
| 2 jobs | Workflows | Listed as `seed_theater_data` and `build_denormalized_theater_sales` |
| 1 Lakeview dashboard | `/Workspace/Users/<user name>/fde_interview/` | Listed as `Theater Performance` |
| 1 Databricks App | `/Workspace/Users/<user name>/fde_interview/files/src/app` |

## 5. Deploy and start the provided application

The scaffold for a Python application has been provided to you in this bundle.  If you have chosen to build an application you should use this as a starting point.  Start the app with:

`databricks apps deploy -t dev -p my-profile`

Additional commands for managing the lifecycle of your application can be found at https://docs.databricks.com/aws/en/dev-tools/cli/reference/apps-commands

## Troubleshooting

See `FAQ.md`.
