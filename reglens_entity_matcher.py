from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

import pandas as pd


CHUNK_ROWS = 200_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize openFDA FAERS + enforcement files into shared entity-keyed "
            "tables for RegLens feature engineering."
        )
    )
    parser.add_argument("--event-path", type=Path, required=True)
    parser.add_argument("--enforcement-path", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("reglens_match_output"))
    parser.add_argument(
        "--entity-level",
        choices=["ingredient", "application", "product"],
        default="product",
        help="Entity key used to align FAERS and recall records.",
    )
    parser.add_argument(
        "--allow-text-product-fallback",
        action="store_true",
        help=(
            "Only used for entity-level=product. Falls back to free-text product names "
            "when product_ndc is missing. This increases coverage but also noise."
        ),
    )
    parser.add_argument(
        "--suspect-only",
        action="store_true",
        help="Keep only FAERS drugs with drugcharacterization == 1 (suspect drug).",
    )
    parser.add_argument(
        "--max-event-records",
        type=int,
        default=None,
        help="Optional cap for debugging on a subset of FAERS reports.",
    )
    parser.add_argument(
        "--max-enforcement-records",
        type=int,
        default=None,
        help="Optional cap for debugging on a subset of enforcement records.",
    )
    parser.add_argument(
        "--output-format",
        choices=["parquet", "csv"],
        default="parquet",
    )
    return parser.parse_args()


def join_list(value: Any) -> str | None:
    if isinstance(value, list):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        return " | ".join(cleaned) if cleaned else None
    if value in (None, ""):
        return None
    return str(value).strip()


def as_clean_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_application(value: Any) -> str | None:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    return normalized.replace(" ", "")


def normalize_product_ndc(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.lower()
    text = re.sub(r"[^0-9-]+", "", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or None


def parse_yyyymmdd(value: Any) -> pd.Timestamp | pd.NaT:
    if value in (None, "", "nan"):
        return pd.NaT
    text = str(value).strip()
    if not re.fullmatch(r"\d{8}", text):
        return pd.NaT
    return pd.to_datetime(text, format="%Y%m%d", errors="coerce")


def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def detect_format(path: Path) -> str:
    suffixes = [suffix.lower() for suffix in path.suffixes]
    if suffixes[-2:] == [".json", ".gz"]:
        return "json"
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        return "jsonl"
    if path.suffix.lower() == ".json":
        return "json"
    raise ValueError(f"Unsupported file format for {path}")


def iter_jsonl_records(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def iter_openfda_bulk_results(path: Path, chunk_size: int = 1_000_000) -> Iterator[dict[str, Any]]:
    decoder = json.JSONDecoder()
    buffer = ""
    inside_results = False

    with path.open("r", encoding="utf-8") as handle:
        eof = False
        while True:
            if not eof:
                chunk = handle.read(chunk_size)
                if chunk:
                    buffer += chunk
                else:
                    eof = True

            if not inside_results:
                key_index = buffer.find('"results"')
                if key_index == -1:
                    if eof:
                        raise ValueError(f"Could not find results array in {path}")
                    buffer = buffer[-100:]
                    continue
                array_start = buffer.find("[", key_index)
                if array_start == -1:
                    if eof:
                        raise ValueError(f"Could not find start of results array in {path}")
                    buffer = buffer[key_index:]
                    continue
                buffer = buffer[array_start + 1 :]
                inside_results = True

            made_progress = False
            while True:
                buffer = buffer.lstrip()
                if not buffer:
                    break
                if buffer[0] == "]":
                    return
                if buffer[0] == ",":
                    buffer = buffer[1:]
                    made_progress = True
                    continue
                try:
                    record, offset = decoder.raw_decode(buffer)
                except json.JSONDecodeError:
                    break
                yield record
                buffer = buffer[offset:]
                made_progress = True

            if eof:
                if not buffer.strip():
                    return
                if not made_progress:
                    raise ValueError(f"Could not decode trailing JSON from {path}")


def iter_records(path: Path) -> Iterator[dict[str, Any]]:
    file_format = detect_format(path)
    if file_format == "jsonl":
        yield from iter_jsonl_records(path)
    else:
        yield from iter_openfda_bulk_results(path)


@dataclass
class EntityStats:
    entity_level: str
    entity_key: str
    entity_name: str | None = None
    entity_match_basis: str | None = None
    in_faers: bool = False
    in_enforcement: bool = False
    faers_event_rows: int = 0
    faers_report_count: int = 0
    serious_event_rows: int = 0
    death_event_rows: int = 0
    first_faers_date: pd.Timestamp | pd.NaT = pd.NaT
    last_faers_date: pd.Timestamp | pd.NaT = pd.NaT
    enforcement_event_rows: int = 0
    first_recall_date: pd.Timestamp | pd.NaT = pd.NaT
    last_recall_date: pd.Timestamp | pd.NaT = pd.NaT
    recall_class_i_count: int = 0
    recall_class_ii_count: int = 0
    recall_class_iii_count: int = 0
    example_faers_name: str | None = None
    example_recall_name: str | None = None
    faers_report_ids_seen: set[str] = field(default_factory=set)

    def update_name(self, name: str | None, basis: str | None) -> None:
        if self.entity_name is None and name:
            self.entity_name = name
        if self.entity_match_basis is None and basis:
            self.entity_match_basis = basis

    def update_faers(
        self,
        report_id: str | None,
        report_date: pd.Timestamp | pd.NaT,
        serious: Any,
        seriousnessdeath: Any,
        display_name: str | None,
        basis: str | None,
    ) -> None:
        self.in_faers = True
        self.update_name(display_name, basis)
        if self.example_faers_name is None and display_name:
            self.example_faers_name = display_name
        self.faers_event_rows += 1
        if report_id and report_id not in self.faers_report_ids_seen:
            self.faers_report_ids_seen.add(report_id)
            self.faers_report_count += 1
        if str(serious) == "1":
            self.serious_event_rows += 1
        if str(seriousnessdeath) == "1":
            self.death_event_rows += 1
        if pd.notna(report_date):
            if pd.isna(self.first_faers_date) or report_date < self.first_faers_date:
                self.first_faers_date = report_date
            if pd.isna(self.last_faers_date) or report_date > self.last_faers_date:
                self.last_faers_date = report_date

    def update_enforcement(
        self,
        recall_date: pd.Timestamp | pd.NaT,
        classification: str | None,
        display_name: str | None,
        basis: str | None,
    ) -> None:
        self.in_enforcement = True
        self.update_name(display_name, basis)
        if self.example_recall_name is None and display_name:
            self.example_recall_name = display_name
        self.enforcement_event_rows += 1
        normalized_class = (classification or "").strip().lower()
        if normalized_class == "class i":
            self.recall_class_i_count += 1
        elif normalized_class == "class ii":
            self.recall_class_ii_count += 1
        elif normalized_class == "class iii":
            self.recall_class_iii_count += 1
        if pd.notna(recall_date):
            if pd.isna(self.first_recall_date) or recall_date < self.first_recall_date:
                self.first_recall_date = recall_date
            if pd.isna(self.last_recall_date) or recall_date > self.last_recall_date:
                self.last_recall_date = recall_date

    def to_row(self) -> dict[str, Any]:
        return {
            "entity_level": self.entity_level,
            "entity_key": self.entity_key,
            "entity_name": self.entity_name,
            "entity_match_basis": self.entity_match_basis,
            "in_faers": self.in_faers,
            "in_enforcement": self.in_enforcement,
            "matched_in_both": self.in_faers and self.in_enforcement,
            "faers_event_rows": self.faers_event_rows,
            "faers_report_count": self.faers_report_count,
            "serious_event_rows": self.serious_event_rows,
            "death_event_rows": self.death_event_rows,
            "first_faers_date": self.first_faers_date,
            "last_faers_date": self.last_faers_date,
            "enforcement_event_rows": self.enforcement_event_rows,
            "first_recall_date": self.first_recall_date,
            "last_recall_date": self.last_recall_date,
            "recall_class_i_count": self.recall_class_i_count,
            "recall_class_ii_count": self.recall_class_ii_count,
            "recall_class_iii_count": self.recall_class_iii_count,
            "example_faers_name": self.example_faers_name,
            "example_recall_name": self.example_recall_name,
        }


class ChunkWriter:
    def __init__(self, output_dir: Path, stem: str, output_format: str) -> None:
        self.output_dir = output_dir
        self.stem = stem
        self.output_format = output_format
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.buffer: list[dict[str, Any]] = []
        self.part = 0
        self.paths: list[Path] = []

    def add(self, row: dict[str, Any]) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= CHUNK_ROWS:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        df = pd.DataFrame(self.buffer)
        suffix = ".parquet" if self.output_format == "parquet" else ".csv"
        path = self.output_dir / f"{self.stem}_part-{self.part:05d}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.output_format == "parquet":
            df.to_parquet(path, index=False)
        else:
            df.to_csv(path, index=False)
        self.paths.append(path)
        self.buffer.clear()
        self.part += 1

    def close(self) -> list[Path]:
        self.flush()
        return self.paths


def dedupe_candidates(
    candidates: list[tuple[str | None, str | None, str | None]]
) -> list[tuple[str, str | None, str | None]]:
    seen: set[str] = set()
    deduped: list[tuple[str, str | None, str | None]] = []
    for entity_key, entity_name, match_basis in candidates:
        if entity_key is None or entity_key in seen:
            continue
        seen.add(entity_key)
        deduped.append((entity_key, entity_name, match_basis))
    return deduped


def build_event_rows(
    record: dict[str, Any],
    entity_level: str,
    allow_text_product_fallback: bool,
    suspect_only: bool,
) -> Iterable[dict[str, Any]]:
    patient = record.get("patient", {}) or {}
    drugs = patient.get("drug", []) or []
    report_date = parse_yyyymmdd(record.get("receiptdate"))
    received_date = parse_yyyymmdd(record.get("receivedate"))
    transmission_date = parse_yyyymmdd(record.get("transmissiondate"))
    primarysource = record.get("primarysource", {}) or {}
    sender = record.get("sender", {}) or {}
    receiver = record.get("receiver", {}) or {}
    reactions = patient.get("reaction", []) or []
    reaction_terms = [
        reaction.get("reactionmeddrapt")
        for reaction in reactions
        if isinstance(reaction, dict) and reaction.get("reactionmeddrapt")
    ]

    for drug_index, drug in enumerate(drugs):
        drug_characterization = str(drug.get("drugcharacterization") or "").strip()
        if suspect_only and drug_characterization != "1":
            continue

        openfda = drug.get("openfda", {}) or {}
        application_numbers = as_clean_list(openfda.get("application_number"))
        substance_names = as_clean_list(openfda.get("substance_name"))
        generic_names = as_clean_list(openfda.get("generic_name"))
        product_ndcs = as_clean_list(openfda.get("product_ndc"))
        application_display = first_non_empty(
            join_list(application_numbers),
            drug.get("drugauthorizationnumb"),
        )
        ingredient_display = first_non_empty(
            join_list(substance_names),
            join_list(generic_names),
        )
        product_ndc_display = join_list(product_ndcs)
        product_text_display = drug.get("medicinalproduct")
        candidates: list[tuple[str | None, str | None, str | None]] = []

        if entity_level == "product":
            for ndc in product_ndcs:
                candidates.append((normalize_product_ndc(ndc), ndc, "product_ndc"))
            if not candidates and allow_text_product_fallback:
                candidates.append(
                    (
                        normalize_text(product_text_display),
                        product_text_display,
                        "product_text_fallback",
                    )
                )
        elif entity_level == "application":
            for application_number in application_numbers:
                candidates.append(
                    (
                        normalize_application(application_number),
                        application_number,
                        "application",
                    )
                )
            if not candidates and drug.get("drugauthorizationnumb"):
                candidates.append(
                    (
                        normalize_application(drug.get("drugauthorizationnumb")),
                        str(drug.get("drugauthorizationnumb")),
                        "drugauthorization_fallback",
                    )
                )
        else:
            if substance_names:
                for substance_name in substance_names:
                    candidates.append(
                        (
                            normalize_text(substance_name),
                            substance_name,
                            "ingredient",
                        )
                    )
            elif generic_names:
                for generic_name in generic_names:
                    candidates.append(
                        (
                            normalize_text(generic_name),
                            generic_name,
                            "generic_name_fallback",
                        )
                    )

        for entity_key, entity_name, match_basis in dedupe_candidates(candidates):
            yield {
                "entity_level": entity_level,
                "entity_key": entity_key,
                "entity_name": entity_name,
                "entity_match_basis": match_basis,
                "source": "faers",
                "safetyreportid": record.get("safetyreportid"),
                "companynumb": record.get("companynumb"),
                "fulfillexpeditecriteria": record.get("fulfillexpeditecriteria"),
                "drug_index": drug_index,
                "report_receiptdate": record.get("receiptdate"),
                "report_receiptdate_parsed": report_date,
                "receivedate": record.get("receivedate"),
                "receivedate_parsed": received_date,
                "transmissiondate": record.get("transmissiondate"),
                "transmissiondate_parsed": transmission_date,
                "serious": record.get("serious"),
                "seriousnessdeath": record.get("seriousnessdeath"),
                "patient_death_date": join_list((patient.get("patientdeath") or {}).get("patientdeathdate")),
                "patientsex": patient.get("patientsex"),
                "patientonsetage": patient.get("patientonsetage"),
                "patientonsetageunit": patient.get("patientonsetageunit"),
                "reaction_count": len(reaction_terms),
                "reaction_terms": join_list(reaction_terms),
                "primarysource_qualification": primarysource.get("qualification"),
                "primarysource_country": primarysource.get("reportercountry"),
                "sender_type": sender.get("sendertype"),
                "sender_organization": sender.get("senderorganization"),
                "receiver_type": receiver.get("receivertype"),
                "drugcharacterization": drug_characterization,
                "medicinalproduct": drug.get("medicinalproduct"),
                "drugindication": drug.get("drugindication"),
                "drugadministrationroute": drug.get("drugadministrationroute"),
                "drugauthorizationnumb": drug.get("drugauthorizationnumb"),
                "openfda_application_number": join_list(application_numbers),
                "openfda_product_ndc": product_ndc_display,
                "openfda_brand_name": join_list(openfda.get("brand_name")),
                "openfda_generic_name": join_list(generic_names),
                "openfda_substance_name": join_list(substance_names),
                "openfda_manufacturer_name": join_list(openfda.get("manufacturer_name")),
                "openfda_product_type": join_list(openfda.get("product_type")),
                "openfda_route": join_list(openfda.get("route")),
                "openfda_package_ndc": join_list(openfda.get("package_ndc")),
                "openfda_rxcui": join_list(openfda.get("rxcui")),
                "openfda_spl_set_id": join_list(openfda.get("spl_set_id")),
                "openfda_spl_id": join_list(openfda.get("spl_id")),
                "openfda_unii": join_list(openfda.get("unii")),
                "candidate_application_display": application_display,
                "candidate_ingredient_display": ingredient_display,
            }


def build_enforcement_rows(
    record: dict[str, Any],
    entity_level: str,
    allow_text_product_fallback: bool,
) -> list[dict[str, Any]]:
    openfda = record.get("openfda", {}) or {}
    application_numbers = as_clean_list(openfda.get("application_number"))
    substance_names = as_clean_list(openfda.get("substance_name"))
    generic_names = as_clean_list(openfda.get("generic_name"))
    product_ndcs = as_clean_list(openfda.get("product_ndc"))
    application_display = join_list(application_numbers)
    ingredient_display = first_non_empty(
        join_list(substance_names),
        join_list(generic_names),
    )
    product_ndc_display = join_list(product_ndcs)
    package_ndc_display = join_list(openfda.get("package_ndc"))
    product_text_display = record.get("product_description")
    candidates: list[tuple[str | None, str | None, str | None]] = []

    if entity_level == "product":
        for ndc in product_ndcs:
            candidates.append((normalize_product_ndc(ndc), ndc, "product_ndc"))
        if not candidates and allow_text_product_fallback:
            candidates.append(
                (
                    normalize_text(product_text_display),
                    product_text_display,
                    "product_text_fallback",
                )
            )
    elif entity_level == "application":
        for application_number in application_numbers:
            candidates.append(
                (
                    normalize_application(application_number),
                    application_number,
                    "application",
                )
            )
    else:
        if substance_names:
            for substance_name in substance_names:
                candidates.append(
                    (
                        normalize_text(substance_name),
                        substance_name,
                        "ingredient",
                    )
                )
        elif generic_names:
            for generic_name in generic_names:
                candidates.append(
                    (
                        normalize_text(generic_name),
                        generic_name,
                        "generic_name_fallback",
                    )
                )

    rows: list[dict[str, Any]] = []
    recall_initiation_date = parse_yyyymmdd(record.get("recall_initiation_date"))
    center_classification_date = parse_yyyymmdd(record.get("center_classification_date"))
    termination_date = parse_yyyymmdd(record.get("termination_date"))
    for entity_key, entity_name, match_basis in dedupe_candidates(candidates):
        rows.append(
            {
                "entity_level": entity_level,
                "entity_key": entity_key,
                "entity_name": entity_name,
                "entity_match_basis": match_basis,
                "source": "enforcement",
                "event_id": record.get("event_id"),
                "status": record.get("status"),
                "classification": record.get("classification"),
                "product_type": record.get("product_type"),
                "recall_number": record.get("recall_number"),
                "recall_initiation_date": record.get("recall_initiation_date"),
                "recall_initiation_date_parsed": recall_initiation_date,
                "center_classification_date": record.get("center_classification_date"),
                "center_classification_date_parsed": center_classification_date,
                "termination_date": record.get("termination_date"),
                "termination_date_parsed": termination_date,
                "report_date": record.get("report_date"),
                "report_date_parsed": parse_yyyymmdd(record.get("report_date")),
                "voluntary_mandated": record.get("voluntary_mandated"),
                "initial_firm_notification": record.get("initial_firm_notification"),
                "product_description": record.get("product_description"),
                "product_quantity": record.get("product_quantity"),
                "code_info": record.get("code_info"),
                "distribution_pattern": record.get("distribution_pattern"),
                "reason_for_recall": record.get("reason_for_recall"),
                "recalling_firm": record.get("recalling_firm"),
                "address_1": record.get("address_1"),
                "address_2": record.get("address_2"),
                "city": record.get("city"),
                "state": record.get("state"),
                "postal_code": record.get("postal_code"),
                "country": record.get("country"),
                "openfda_application_number": application_display,
                "openfda_product_ndc": product_ndc_display,
                "openfda_package_ndc": package_ndc_display,
                "openfda_brand_name": join_list(openfda.get("brand_name")),
                "openfda_generic_name": join_list(generic_names),
                "openfda_substance_name": join_list(substance_names),
                "openfda_manufacturer_name": join_list(openfda.get("manufacturer_name")),
                "openfda_product_type": join_list(openfda.get("product_type")),
                "openfda_route": join_list(openfda.get("route")),
                "openfda_rxcui": join_list(openfda.get("rxcui")),
                "openfda_spl_set_id": join_list(openfda.get("spl_set_id")),
                "openfda_spl_id": join_list(openfda.get("spl_id")),
                "openfda_unii": join_list(openfda.get("unii")),
                "candidate_ingredient_display": ingredient_display,
            }
        )
    return rows


def update_entity_stats_for_faers(
    stats: dict[str, EntityStats],
    row: dict[str, Any],
    entity_level: str,
) -> None:
    entity_key = row["entity_key"]
    current = stats.get(entity_key)
    if current is None:
        current = EntityStats(entity_level=entity_level, entity_key=entity_key)
        stats[entity_key] = current
    current.update_faers(
        report_id=row.get("safetyreportid"),
        report_date=row.get("report_receiptdate_parsed"),
        serious=row.get("serious"),
        seriousnessdeath=row.get("seriousnessdeath"),
        display_name=row.get("entity_name"),
        basis=row.get("entity_match_basis"),
    )


def update_entity_stats_for_enforcement(
    stats: dict[str, EntityStats],
    row: dict[str, Any],
    entity_level: str,
) -> None:
    entity_key = row["entity_key"]
    current = stats.get(entity_key)
    if current is None:
        current = EntityStats(entity_level=entity_level, entity_key=entity_key)
        stats[entity_key] = current
    current.update_enforcement(
        recall_date=row.get("recall_initiation_date_parsed"),
        classification=row.get("classification"),
        display_name=row.get("entity_name"),
        basis=row.get("entity_match_basis"),
    )


def write_table(df: pd.DataFrame, path: Path, output_format: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)


def build_summary_tables(stats: dict[str, EntityStats]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    registry_df = pd.DataFrame([entity.to_row() for entity in stats.values()])
    if registry_df.empty:
        matched_df = registry_df.copy()
        faers_universe_df = registry_df.copy()
    else:
        registry_df = registry_df.sort_values(
            ["matched_in_both", "faers_event_rows", "enforcement_event_rows", "entity_key"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        matched_df = registry_df[registry_df["matched_in_both"]].reset_index(drop=True)
        faers_universe_df = registry_df[registry_df["in_faers"]].reset_index(drop=True)
    return registry_df, matched_df, faers_universe_df


def build_run_summary(
    *,
    entity_level: str,
    suspect_only: bool,
    allow_text_product_fallback: bool,
    event_records_seen: int,
    enforcement_records_seen: int,
    faers_rows_written: int,
    enforcement_rows_written: int,
    registry_df: pd.DataFrame,
    matched_df: pd.DataFrame,
    faers_chunk_paths: list[Path],
    enforcement_chunk_paths: list[Path],
) -> dict[str, Any]:
    matched_faers_rows = int(matched_df["faers_event_rows"].sum()) if not matched_df.empty else 0
    matched_enforcement_rows = (
        int(matched_df["enforcement_event_rows"].sum()) if not matched_df.empty else 0
    )
    return {
        "entity_level": entity_level,
        "suspect_only": suspect_only,
        "allow_text_product_fallback": allow_text_product_fallback,
        "event_records_seen": event_records_seen,
        "enforcement_records_seen": enforcement_records_seen,
        "faers_entity_event_rows_written": faers_rows_written,
        "enforcement_entity_event_rows_written": enforcement_rows_written,
        "entity_count_total": int(len(registry_df)),
        "entity_count_in_faers": int(registry_df["in_faers"].sum()) if not registry_df.empty else 0,
        "entity_count_in_enforcement": int(registry_df["in_enforcement"].sum()) if not registry_df.empty else 0,
        "entity_count_matched_in_both": int(len(matched_df)),
        "matched_faers_event_rows": matched_faers_rows,
        "matched_enforcement_event_rows": matched_enforcement_rows,
        "faers_event_chunk_files": [str(path) for path in faers_chunk_paths],
        "enforcement_event_chunk_files": [str(path) for path in enforcement_chunk_paths],
    }


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    faers_output_dir = output_dir / "faers_entity_events"
    enforcement_output_dir = output_dir / "enforcement_entity_events"
    summary_output_dir = output_dir / "summaries"

    faers_writer = ChunkWriter(
        output_dir=faers_output_dir,
        stem=f"faers_{args.entity_level}",
        output_format=args.output_format,
    )
    enforcement_writer = ChunkWriter(
        output_dir=enforcement_output_dir,
        stem=f"enforcement_{args.entity_level}",
        output_format=args.output_format,
    )

    entity_stats: dict[str, EntityStats] = {}
    event_records_seen = 0
    enforcement_records_seen = 0
    faers_rows_written = 0
    enforcement_rows_written = 0

    for record in iter_records(args.event_path):
        if args.max_event_records is not None and event_records_seen >= args.max_event_records:
            break
        event_records_seen += 1
        for row in build_event_rows(
            record=record,
            entity_level=args.entity_level,
            allow_text_product_fallback=args.allow_text_product_fallback,
            suspect_only=args.suspect_only,
        ):
            faers_writer.add(row)
            update_entity_stats_for_faers(entity_stats, row, args.entity_level)
            faers_rows_written += 1

    for record in iter_records(args.enforcement_path):
        if (
            args.max_enforcement_records is not None
            and enforcement_records_seen >= args.max_enforcement_records
        ):
            break
        enforcement_records_seen += 1
        rows = build_enforcement_rows(
            record=record,
            entity_level=args.entity_level,
            allow_text_product_fallback=args.allow_text_product_fallback,
        )
        for row in rows:
            enforcement_writer.add(row)
            update_entity_stats_for_enforcement(entity_stats, row, args.entity_level)
            enforcement_rows_written += 1

    faers_chunk_paths = faers_writer.close()
    enforcement_chunk_paths = enforcement_writer.close()

    registry_df, matched_df, faers_universe_df = build_summary_tables(entity_stats)
    suffix = ".parquet" if args.output_format == "parquet" else ".csv"
    registry_path = summary_output_dir / f"entity_registry_{args.entity_level}{suffix}"
    matched_path = summary_output_dir / f"matched_entities_{args.entity_level}{suffix}"
    faers_universe_path = summary_output_dir / f"faers_universe_{args.entity_level}{suffix}"
    write_table(registry_df, registry_path, args.output_format)
    write_table(matched_df, matched_path, args.output_format)
    write_table(faers_universe_df, faers_universe_path, args.output_format)

    run_summary = build_run_summary(
        entity_level=args.entity_level,
        suspect_only=args.suspect_only,
        allow_text_product_fallback=args.allow_text_product_fallback,
        event_records_seen=event_records_seen,
        enforcement_records_seen=enforcement_records_seen,
        faers_rows_written=faers_rows_written,
        enforcement_rows_written=enforcement_rows_written,
        registry_df=registry_df,
        matched_df=matched_df,
        faers_chunk_paths=faers_chunk_paths,
        enforcement_chunk_paths=enforcement_chunk_paths,
    )
    run_summary_path = summary_output_dir / f"run_summary_{args.entity_level}.json"
    run_summary_path.parent.mkdir(parents=True, exist_ok=True)
    run_summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

    print(f"Output directory: {output_dir}")
    print(f"Entity level: {args.entity_level}")
    print(f"FAERS records seen: {event_records_seen}")
    print(f"Enforcement records seen: {enforcement_records_seen}")
    print(f"FAERS entity-event rows written: {faers_rows_written}")
    print(f"Enforcement entity-event rows written: {enforcement_rows_written}")
    print(f"Entity registry rows: {len(registry_df)}")
    print(f"Matched entities: {len(matched_df)}")
    print(f"FAERS event chunks: {len(faers_chunk_paths)}")
    print(f"Enforcement event chunks: {len(enforcement_chunk_paths)}")
    print(f"Registry: {registry_path}")
    print(f"Matched entities: {matched_path}")
    print(f"FAERS universe: {faers_universe_path}")
    print(f"Run summary: {run_summary_path}")


if __name__ == "__main__":
    main()
