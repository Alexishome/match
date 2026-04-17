# RegLens openFDA Matcher

This repository contains a matching module for the RegLens course project.

The main script is:
- `reglens_entity_matcher.py`

Its job is to align local openFDA FAERS and enforcement data onto a shared entity key so the next teammate can build time-window features and future recall labels without re-parsing the raw JSON.

## Current default

The default matching behavior is intentionally strict:
- `--entity-level product`
- structured `openfda.product_ndc` matching
- no free-text product fallback by default

This is the right default for validating how far product/NDC matching can realistically go before introducing looser heuristics.

## What the script writes

For an output directory like `out_dir`, the script writes:
- `out_dir/faers_entity_events/`
- `out_dir/enforcement_entity_events/`
- `out_dir/summaries/entity_registry_product.*`
- `out_dir/summaries/matched_entities_product.*`
- `out_dir/summaries/faers_universe_product.*`
- `out_dir/summaries/run_summary_product.json`

The output is designed as a feature-engineering-ready intermediate layer, not a final model table.

## Entity levels supported

- `product`
- `application`
- `ingredient`

Default is `product`.

## Why this version is richer

This script keeps more low-level fields than a minimal matcher so the feature-engineering teammate has more flexibility later.

### FAERS fields retained

Examples of retained FAERS fields:
- report identifiers and timing: `safetyreportid`, `companynumb`, `report_receiptdate`, `receivedate`, `transmissiondate`
- seriousness and patient context: `serious`, `seriousnessdeath`, `patientsex`, `patientonsetage`, `patientonsetageunit`, `patient_death_date`
- reaction summary: `reaction_count`, `reaction_terms`
- report-source metadata: `primarysource_qualification`, `primarysource_country`, `sender_type`, `sender_organization`, `receiver_type`
- drug-level context: `drugcharacterization`, `medicinalproduct`, `drugindication`, `drugadministrationroute`, `drugauthorizationnumb`
- harmonized identifiers: `openfda_product_ndc`, `openfda_package_ndc`, `openfda_application_number`, `openfda_brand_name`, `openfda_generic_name`, `openfda_substance_name`, `openfda_manufacturer_name`, `openfda_route`, `openfda_product_type`, `openfda_rxcui`, `openfda_spl_set_id`, `openfda_spl_id`, `openfda_unii`

### Enforcement fields retained

Examples of retained enforcement fields:
- recall identifiers and timing: `event_id`, `recall_number`, `recall_initiation_date`, `center_classification_date`, `termination_date`, `report_date`
- recall severity and status: `classification`, `status`, `voluntary_mandated`, `initial_firm_notification`
- logistics and scope hints: `product_quantity`, `code_info`, `distribution_pattern`, `reason_for_recall`
- firm and geography: `recalling_firm`, `address_1`, `address_2`, `city`, `state`, `postal_code`, `country`
- product text: `product_description`, `product_type`
- harmonized identifiers: `openfda_product_ndc`, `openfda_package_ndc`, `openfda_application_number`, `openfda_brand_name`, `openfda_generic_name`, `openfda_substance_name`, `openfda_manufacturer_name`, `openfda_route`, `openfda_product_type`, `openfda_rxcui`, `openfda_spl_set_id`, `openfda_spl_id`, `openfda_unii`

## Usage

### Strict product/NDC matching

```bash
python3 reglens_entity_matcher.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir
```

### Suspect-only FAERS

```bash
python3 reglens_entity_matcher.py \
  --event-path /path/to/drug-event.jsonl \
  --enforcement-path /path/to/drug-enforcement.json \
  --output-dir /path/to/output_dir \
  --suspect-only
```

### Product mode with text fallback

This increases coverage and noise.

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

## Validation on the existing local sample

The current committed sample result was regenerated after expanding the retained field set.

Validation data:
- 2,000 FAERS reports
- 2,000 enforcement records
- strict product/NDC mode

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
- matched shared entity keys: `249`
- matched FAERS entity-event rows: `1118`
- matched enforcement entity-event rows: `501`

Committed example artifacts:
- `examples/validation_sample_results.md`
- `examples/matched_entities_sample.csv`
- `examples/faers_event_sample.csv`
- `examples/enforcement_event_sample.csv`

The event-level sample CSVs now show richer retained fields, not just the minimal matching columns.

## Important caveats

- FAERS is a report dataset, not a clean product master table.
- A single FAERS drug row can expand to many `product_ndc` values.
- Strict product/NDC mode can greatly inflate FAERS entity-event rows.
- Enforcement rows often lack harmonized identifiers, so strict overlap can stay sparse even when the pipeline works technically.

This repository is meant to create a feature-engineering-ready intermediate layer, not the final modeling dataset itself.
