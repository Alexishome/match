# RegLens openFDA Matcher

This repository contains two versions of the RegLens matching module:
- `reglens_entity_matcher_fast.py`
- `reglens_entity_matcher.py`

Both scripts align local openFDA FAERS and enforcement data onto a shared entity key so the next teammate can build time-window features and future recall labels without re-parsing the raw JSON.

## Which version to use

### Fast version

File:
- `reglens_entity_matcher_fast.py`

Use this when:
- the data volume is large and runtime/output size matter more
- the team wants to validate strict matching behavior first
- the next step only needs core fields

Characteristics:
- same core matching logic
- leaner retained-field set
- smaller output tables
- better choice for quick overlap checks on bigger local dumps

### Rich version

File:
- `reglens_entity_matcher.py`

Use this when:
- matching logic is already acceptable
- the next teammate is about to do feature engineering
- you want to preserve optional fields and avoid re-parsing raw JSON later

Characteristics:
- same core matching logic
- more retained FAERS and enforcement fields
- larger output tables
- better handoff for downstream feature work

## Current default behavior

Both versions default to:
- `--entity-level product`
- structured `openfda.product_ndc` matching
- no free-text product fallback by default

Supported entity levels:
- `product`
- `application`
- `ingredient`

## Output structure

For an output directory like `out_dir`, both versions write:
- `out_dir/faers_entity_events/`
- `out_dir/enforcement_entity_events/`
- `out_dir/summaries/entity_registry_product.*`
- `out_dir/summaries/matched_entities_product.*`
- `out_dir/summaries/faers_universe_product.*`
- `out_dir/summaries/run_summary_product.json`

The output is a feature-engineering-ready intermediate layer, not a final model table.

## Field coverage difference

### Fast version keeps core fields

FAERS-side core fields include:
- `entity_key`
- `safetyreportid`
- `report_receiptdate`
- `serious`
- `seriousnessdeath`
- `drugcharacterization`
- `medicinalproduct`
- `drugindication`
- `drugadministrationroute`
- core `openfda_*` identifiers

Enforcement-side core fields include:
- `event_id`
- `status`
- `classification`
- `recall_initiation_date`
- `center_classification_date`
- `termination_date`
- `product_description`
- `reason_for_recall`
- `recalling_firm`
- core `openfda_*` identifiers

### Rich version keeps extra feature-base fields

Extra FAERS-style fields retained:
- `companynumb`
- `fulfillexpeditecriteria`
- `receivedate`
- `transmissiondate`
- `patient_death_date`
- `patientsex`
- `patientonsetage`
- `patientonsetageunit`
- `reaction_count`
- `reaction_terms`
- `primarysource_qualification`
- `primarysource_country`
- `sender_type`
- `sender_organization`
- `receiver_type`
- extra harmonized fields such as `openfda_package_ndc`, `openfda_route`, `openfda_rxcui`, `openfda_spl_set_id`, `openfda_spl_id`, `openfda_unii`

Extra enforcement-side fields retained:
- `product_type`
- `report_date`
- `voluntary_mandated`
- `initial_firm_notification`
- `product_quantity`
- `code_info`
- `distribution_pattern`
- `address_1`
- `address_2`
- `city`
- `state`
- `postal_code`
- `country`
- extra harmonized fields such as `openfda_package_ndc`, `openfda_route`, `openfda_rxcui`, `openfda_spl_set_id`, `openfda_spl_id`, `openfda_unii`

## Usage

### Fast strict product/NDC matching

```bash
python3 reglens_entity_matcher_fast.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir
```

### Rich strict product/NDC matching

```bash
python3 reglens_entity_matcher.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir
```

### Suspect-only FAERS

```bash
python3 reglens_entity_matcher_fast.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir \
  --suspect-only
```

### Product mode with text fallback

This increases coverage and noise.

```bash
python3 reglens_entity_matcher_fast.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir \
  --allow-text-product-fallback
```

### Ingredient-level fallback

```bash
python3 reglens_entity_matcher_fast.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir \
  --entity-level ingredient
```

## Sample results committed in this repo

Both sample runs use the same existing local exploratory data:
- 2,000 FAERS reports
- 2,000 enforcement records
- strict `product / NDC` matching

### Fast sample

Validation command:

```bash
python3 reglens_entity_matcher_fast.py \
  --event-path ../reglens_openfda_output/raw/drug_event.jsonl \
  --enforcement-path ../drug-enforcement-0001-of-0001.json \
  --output-dir validation_output/sample_product_ndc_fast \
  --max-event-records 2000 \
  --max-enforcement-records 2000 \
  --output-format csv
```

Committed fast sample artifacts:
- `examples/fast_validation_sample_results.md`
- `examples/fast_matched_entities_sample.csv`
- `examples/fast_faers_event_sample.csv`
- `examples/fast_enforcement_event_sample.csv`

### Rich sample

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

Committed rich sample artifacts:
- `examples/rich_validation_sample_results.md`
- `examples/rich_matched_entities_sample.csv`
- `examples/rich_faers_event_sample.csv`
- `examples/rich_enforcement_event_sample.csv`

### Shared sample overlap result

On this 2,000 + 2,000 sample:
- matched shared entity keys: `249`
- matched FAERS entity-event rows: `1118`
- matched enforcement entity-event rows: `501`

The overlap result is the same because the matching logic is the same. The difference is the retained field set and the resulting output weight.

## Important caveats

- FAERS is a report dataset, not a clean product master table.
- A single FAERS drug row can expand to many `product_ndc` values.
- Strict product/NDC mode can greatly inflate FAERS entity-event rows.
- Enforcement rows often lack harmonized identifiers, so strict overlap can stay sparse even when the pipeline works technically.

This repository is meant to create a feature-engineering-ready intermediate layer, not the final modeling dataset itself.
