# stock-data-etl-pipeline
A data pipeline to extract, transform and load historic financial statements

## Architecture overview
This repository powers a scalable ETL workflow that ingests decades of financial metrics for thousands of public companies so analysts and dashboards can query pre-computed results quickly. The process starts when an admin requests a stock: the API checks Postgres/Redis, enqueues fetch jobs via RabbitMQ, and Celery workers retrieve raw data from QuickFS before storing it in S3. Delta tables are created by downstream Celery workers and served through Trino-backed views, letting Superset present up-to-date dashboards without hammering the origin API or the data lake. Notifications, retry handling, and admin UI state all live alongside the queueing system, so the workflow is resilient and observable.

![Architecture Diagram](/docs/architecture.png)

## Goals
- Provide a single source for 20+ years of financial metrics for ~20,000 stocks
- Cache results through Redis/Postgres so that Superset queries hit fast Trino views
- Store raw and delta tables in S3 with Celery workers orchestrating the transforms
- Surface processing status and notifications back in the admin UI for operators

## Getting started
1. Clone the repo:
   ```bash
   git clone git@github.com:raulstechtips/stock-data-etl-pipeline.git
   ```
2. Open the directory and start the recommended dev container:
   ```bash
   cd stock-data-etl-pipeline
   ```
   Use your IDE to reopen in the `.devcontainer`, which provisions Docker containers, VS Code settings, and any required extensions.
3. Install dependencies and configure credentials inside the dev container as documented in `services/config/`.
4. Run the test suite or boot services via the existing Docker Compose/Celery setups to verify the pipeline is working before pushing changes.

## Next steps (in progress)
1. Wire up Superset and the Trino connector within Kubernetes so dashboards can query the delta tables through a secured gateway.
2. Continue building the admin UI so operators can kick off stock fetches, monitor queue status, and view notifications in a single interface.
