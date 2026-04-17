# Validation Sample Results

Validation run used the existing local exploratory sample data:
- 2,000 FAERS reports
- 2,000 enforcement records
- entity level: `product`
- strict matching: structured `openfda.product_ndc` only

## Key counts

- `faers_entity_event_rows_written`: 131238
- `enforcement_entity_event_rows_written`: 1324
- `entity_count_total`: 18557
- `entity_count_in_faers`: 18052
- `entity_count_in_enforcement`: 754
- `entity_count_matched_in_both`: 249
- `matched_faers_event_rows`: 1118
- `matched_enforcement_event_rows`: 501

## Interpretation

- Strict product/NDC matching is workable technically, but overlap is sparse.
- FAERS expands heavily because one drug row can contain many product NDCs.
- The matched entity sample below is meant as a sanity check, not a full-data estimate.

## Matched Entity Sample

```text
entity_key  faers_event_rows  faers_report_count  serious_event_rows  death_event_rows  enforcement_event_rows first_faers_date last_faers_date first_recall_date last_recall_date example_faers_name example_recall_name
 0904-6730                22                  21                  19                15                       1       2014-03-12      2015-08-18        2024-05-20       2024-05-20          0904-6730           0904-6730
 36000-306                22                  21                  19                15                       1       2014-03-12      2015-08-18        2025-08-20       2025-08-20          36000-306           36000-306
 36000-372                22                  21                  19                15                       1       2014-03-12      2015-08-18        2025-08-20       2025-08-20          36000-372           36000-372
 55150-307                22                  21                  19                15                       1       2014-03-12      2015-08-18        2020-12-30       2020-12-30          55150-307           55150-307
 72288-405                22                  21                  19                15                       1       2014-03-12      2015-08-18        2025-05-22       2025-05-22          72288-405           72288-405
 82673-096                22                  21                  19                15                       1       2014-03-12      2015-08-18        2023-11-13       2023-11-13          82673-096           82673-096
 0054-3294                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-3294           0054-3294
 0054-3298                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-3298           0054-3298
 0054-4297                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-4297           0054-4297
 0054-4299                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-4299           0054-4299
 0054-4301                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-4301           0054-4301
 0054-8297                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-8297           0054-8297
 0054-8299                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-8299           0054-8299
 0054-8301                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2016-12-22       2016-12-22          0054-8301           0054-8301
 64980-562                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2026-01-10       2026-01-10          64980-562           64980-562
 64980-563                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2026-01-10       2026-01-10          64980-563           64980-563
 64980-564                18                  18                  11                 0                       1       2014-03-12      2017-01-19        2026-01-10       2026-01-10          64980-564           64980-564
 0069-0468                14                  11                   4                 0                       2       2014-03-12      2014-03-25        2021-03-15       2021-08-13          0069-0468           0069-0468
 0069-0469                14                  11                   4                 0                       2       2014-03-12      2014-03-25        2021-03-15       2021-08-13          0069-0469           0069-0469
 0069-0471                14                  11                   4                 0                       2       2014-03-12      2014-03-25        2021-03-15       2021-08-13          0069-0471           0069-0471
```

## Example Output Files

- `examples/matched_entities_sample.csv`
- `examples/faers_event_sample.csv`
- `examples/enforcement_event_sample.csv`