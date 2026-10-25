# Convenience commands. Most expect `set -a; source .env; set +a` first.
SHELL := /bin/bash
PY ?= python
CRITEO_FILE ?= data/criteo_attribution_dataset.tsv.gz
RAW ?= s3://$(S3_BUCKET)/raw/criteo
CLEAN ?= s3://$(S3_BUCKET)/clean/criteo
DAY ?= 2025-01-05

.PHONY: help sample land-local land-s3 spark-day spark-local benchmark test snowflake-setup \
        dbt-deps dbt-dev dbt-docs astro-start backfill backfill-day \
        kafka-up kafka-down stream-produce stream-run stream-local test-streaming contract-check snowflake-streaming

help:
	@grep -E '^[a-z-]+:.*?##' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-16s %s\n", $$1, $$2}'

sample: ## make a 500K-row synthetic Criteo file for local dev
	$(PY) include/scripts/generate_sample_data.py --rows 500000 --out data/sample.tsv.gz

land-local: ## land the sample into data/landing (no AWS needed)
	$(PY) include/scripts/land_raw_to_s3.py --src data/sample.tsv.gz --dest data/landing

spark-local: ## clean one day locally
	$(PY) include/spark/clean_impressions.py --raw-root data/landing --clean-root data/clean --dt $(DAY)

land-s3: ## land the REAL Criteo file into S3, partitioned by day
	$(PY) include/scripts/land_raw_to_s3.py --src $(CRITEO_FILE) --dest $(RAW)

spark-day: ## clean one day from S3 to S3
	$(PY) include/spark/clean_impressions.py --raw-root $(RAW) --clean-root $(CLEAN) --dt $(DAY)

benchmark: ## before/after Spark timings on the real data (writes docs/benchmark_results.md)
	$(PY) include/spark/benchmark.py --raw-root $(RAW) --clean-root s3://$(S3_BUCKET)/bench/criteo \
	  --repeats 3 --out docs/benchmark_results.md

test: ## Spark tests locally (DAG tests: astro dev pytest)
	$(PY) -m pytest tests/test_spark_clean.py -q

snowflake-streaming: ## create the streaming tables in RAW (after snowflake-setup)
	snow sql -f include/snowflake/03_streaming.sql

snowflake-setup: ## run the three setup scripts with Snowflake CLI (needs SVC_PUBLIC_KEY, AWS_ROLE_ARN, HASH_SALT)
	snow sql -f include/snowflake/00_account_setup.sql -D "svc_public_key=$(SVC_PUBLIC_KEY)"
	snow sql -f include/snowflake/01_s3_integration_and_raw_tables.sql \
	  -D "s3_bucket=$(S3_BUCKET)" -D "aws_role_arn=$(AWS_ROLE_ARN)"
	snow sql -f include/snowflake/02_governance.sql -D "hash_salt=$(HASH_SALT)"

dbt-deps:
	cd include/dbt && DBT_PROFILES_DIR=. dbt deps

dbt-dev: ## build everything into your dev schema
	cd include/dbt && DBT_PROFILES_DIR=. DBT_TARGET=dev dbt build

dbt-docs: ## browse lineage locally
	cd include/dbt && DBT_PROFILES_DIR=. dbt docs generate && DBT_PROFILES_DIR=. dbt docs serve

astro-start: ## start local Airflow (UI on http://localhost:8080)
	astro dev start

backfill: ## load all 31 days through Airflow
	astro dev run backfill create --dag-id ad_analytics_daily \
	  --from-date 2025-01-01 --to-date 2025-01-31 --max-active-runs 4

backfill-day: ## rerun one day end to end (e.g. after fixing a bug): make backfill-day DAY=2025-01-07
	astro dev run backfill create --dag-id ad_analytics_daily \
	  --from-date $(DAY) --to-date $(DAY) --reprocess-behavior completed

# ---------------- streaming: Kafka -> Spark Structured Streaming -> Snowflake ----------------
STREAM_SINK ?= duckdb
kafka-up: ## start a local single-node Kafka (+ UI on :8085)
	docker compose -f docker-compose.streaming.yml up -d --wait

kafka-down:
	docker compose -f docker-compose.streaming.yml down -v

stream-produce: ## replay the sample as events into Kafka, with a few injected faults
	$(PY) include/streaming/producer.py --src data/sample.tsv.gz --bootstrap localhost:9092 \
	  --limit 100000 --rate 2000 --fault-rate 0.01 --dup-rate 0.01 --late-rate 0.005

stream-run: ## run the streaming job against local Kafka (STREAM_SINK=snowflake to load Snowflake)
	$(PY) include/streaming/stream_impressions.py --bootstrap localhost:9092 --sink $(STREAM_SINK) \
	  --campaigns data/landing/campaign_changes/campaign_changes.csv

stream-local: ## no broker needed: Kafka-shaped files -> streaming job -> DuckDB
	$(PY) include/streaming/producer.py --src data/sample.tsv.gz --to-files data/kafka_files --files 5 \
	  --limit 100000 --fault-rate 0.01 --dup-rate 0.01 --late-rate 0.005
	$(PY) include/streaming/stream_impressions.py --source-dir data/kafka_files --sink duckdb \
	  --duckdb-path data/stream_warehouse.duckdb --available-now
	$(PY) -c "import duckdb; c=duckdb.connect('data/stream_warehouse.duckdb', read_only=True); \
	  [print(t, c.execute(f'select count(*) from {t}').fetchone()[0]) for t in \
	   ['IMPRESSIONS_STREAM','IMPRESSIONS_DLQ','CAMPAIGN_MINUTE_METRICS','STREAM_BATCH_METRICS']]"

test-streaming: ## contract + streaming tests (add KAFKA_BOOTSTRAP=localhost:9092 for the real-Kafka test)
	$(PY) -m pytest tests/test_contract.py tests/test_streaming.py tests/integration -q

contract-check: ## would the contract on this branch break consumers of the one on main?
	git show origin/main:include/streaming/contracts/ad_impression_event.yaml > /tmp/contract_main.yaml
	$(PY) include/streaming/check_contract.py /tmp/contract_main.yaml include/streaming/contracts/ad_impression_event.yaml
