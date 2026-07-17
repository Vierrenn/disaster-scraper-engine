import json
from pathlib import Path

SILVER_DIR = Path(__file__).resolve().parent.parent / "data" / "silver"
QUARANTINE_DIR = Path(__file__).resolve().parent.parent / "data" / "quarantine"

REQUIRED_FIELDS = ("title", "body")


def _missing_required_fields(record: dict) -> list[str]:
    return [field for field in REQUIRED_FIELDS if not record.get(field)]


def save_record(record: dict, bronze_path: Path) -> Path:
    record_id = bronze_path.stem
    missing = _missing_required_fields(record)

    if missing:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        path = QUARANTINE_DIR / f"{record_id}.json"
        payload = {
            **record,
            "failure_reason": f"field wajib kosong: {', '.join(missing)}",
        }
    else:
        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        path = SILVER_DIR / f"{record_id}.json"
        payload = record

    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
