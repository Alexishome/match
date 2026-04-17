# RegLens openFDA Matcher

This repository contains a first-pass matching module for the RegLens course project.

The current default is intentionally strict:
- prediction entity defaults to `product`
- product matching uses structured `openfda.product_ndc`
- free-text product fallback is disabled by default

That default is useful for validation because it shows how far strict product/NDC matching can realistically go before adding looser heuristics.

## What the module does

`reglens_entity_matcher.py` reads local FAERS and enforcement files, normalizes them onto a shared entity key, and writes outputs that are ready for downstream feature engineering.

Outputs are split into three layers:
- `faers_entity_events/`: standardized FAERS rows keyed by entity
- `enforcement_entity_events/`: standardized recall rows keyed by entity
- `summaries/`: entity registry and overlap summaries

## Supported inputs

The script is designed for local files that have already been downloaded from openFDA.

Supported formats:
- FAERS: `jsonl`, `ndjson`, or bulk `json`
- Enforcement: bulk `json`, `jsonl`, or `ndjson`

The bulk openFDA format is the one with top-level `meta` and `results`.

## Default matching behavior

Default command-line behavior:
- `--entity-level product`
- strict `product_ndc` matching
- no free-text product fallback

Interpretation by mode:
- `product`: one `product_ndc` becomes one `entity_key`
- `application`: one application number becomes one `entity_key`
- `ingredient`: one substance/generic name becomes one `entity_key`

## Usage

### Strict product/NDC matching

```bash
python3 reglens_entity_matcher.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir
```

### Product matching with suspect drugs only

```bash
python3 reglens_entity_matcher.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir \
  --suspect-only
```

### Product matching with text fallback

This increases coverage but also noise.

```bash
python3 reglens_entity_matcher.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir \
  --allow-text-product-fallback
```

### Ingredient-level fallback

```bash
python3 reglens_entity_matcher.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir \
  --entity-level ingredient
```

## Output files

For `--output-dir out_dir` and `--entity-level product`, the script writes:

- `out_dir/faers_entity_events/faers_product_part-*.parquet`
- `out_dir/enforcement_entity_events/enforcement_product_part-*.parquet`
- `out_dir/summaries/entity_registry_product.parquet`
- `out_dir/summaries/matched_entities_product.parquet`
- `out_dir/summaries/faers_universe_product.parquet`
- `out_dir/summaries/run_summary_product.json`

### Entity registry columns

Main registry fields:
- `entity_key`
- `entity_name`
- `entity_match_basis`
- `in_faers`
- `in_enforcement`
- `matched_in_both`
- `faers_event_rows`
- `faers_report_count`
- `serious_event_rows`
- `death_event_rows`
- `first_faers_date`
- `last_faers_date`
- `enforcement_event_rows`
- `first_recall_date`
- `last_recall_date`
- `recall_class_i_count`
- `recall_class_ii_count`
- `recall_class_iii_count`

These are intentionally kept at the entity-summary level so a downstream pipeline can build time-window features from the event tables.

## Validation on local sample data

This repo was validated on the existing local sample data that had already been downloaded during exploratory work:
- 2,000 FAERS reports from `drug_event`
- 2,000 enforcement records from `drug_enforcement`

Validation command:

```bash
python3 reglens_entity_matcher.py \
  --event-path ../reglens_openfda_output/raw/drug_event.jsonl \
  --enforcement-path ../drug-enforcement-0001-of-0001.json \
  --output-dir validation_output/sample_product_ndc \
  --max-event-records 2000 \
  --max-enforcement-records 2000 \
  --output-format csv
```

Key sample result:
- strict product/NDC mode matched `249` entity keys across the two sampled sources

This number is only a validation sample result, not a full-data estimate.

Committed example artifacts:
- `examples/validation_sample_results.md`
- `examples/matched_entities_sample.csv`
- `examples/faers_event_sample.csv`
- `examples/enforcement_event_sample.csv`

## Important caveats

- FAERS is a report dataset, not a clean product master table.
- A single FAERS drug row can expand to many `product_ndc` values.
- Strict product/NDC mode may greatly increase FAERS entity-event rows because one record can fan out to multiple NDC keys.
- Enforcement rows often lack structured harmonized fields, so strict matching can still be sparse.

This repository is meant to produce a feature-engineering-ready intermediate layer, not a final modeling dataset by itself.
