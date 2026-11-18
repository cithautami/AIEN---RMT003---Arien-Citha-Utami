import csv
import hashlib
import io
import json
from pathlib import Path
from uuid import uuid4

from shared.db import connect


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE_FILES = {
    "operational": [
        "customers.json", "customer_profiles.json", "customer_addresses.json",
        "products.json", "product_categories.json", "stores.json",
        "sales_channels.json", "promotions.json", "orders.json",
        "order_items.json", "order_promotions.json",
    ],
    "events": [
        "payment_events.json", "refund_events.json", "return_events.json",
        "support_events.json", "web_events.json",
    ],
    "inventory": ["inventory_snapshots.csv"],
    "reference": ["campaign_spend.csv", "city_reference.json"],
}


def as_json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def initialize_schemas():
    """Create the participant schemas using the saved SQL definitions."""
    with connect("local") as connection:
        for filename in ("ops.sql", "bronze.sql"):
            path = PROJECT_ROOT / "database" / "schemas" / filename
            connection.execute(path.read_text(encoding="utf-8-sig"))


def validate_raw(raw_dir):
    """Read the 19 required inputs without business cleansing or deduplication."""
    sources = []
    raw_dir = Path(raw_dir)

    for folder, filenames in SOURCE_FILES.items():
        for filename in filenames:
            relative_path = f"{folder}/{filename}"
            path = raw_dir / relative_path
            if not path.is_file():
                raise FileNotFoundError(f"File tidak ditemukan: {path}")

            file_bytes = path.read_bytes()
            content = file_bytes.decode("utf-8-sig")
            if path.suffix == ".json":
                records = json.loads(content)
                if not isinstance(records, list):
                    raise ValueError(f"{relative_path}: JSON harus berupa array/list")
            else:
                records = list(csv.DictReader(io.StringIO(content, newline="")))

            sources.append({
                "source_file": relative_path,
                "file_sha256": hashlib.sha256(file_bytes).hexdigest(),
                "file_size_bytes": len(file_bytes),
                "records": records,
                "loaded_at": None,
            })

    return sources


def load_bronze(sources, run_id):
    """Append missing source positions; never overwrite existing evidence."""
    query = """
        WITH inserted AS (
            INSERT INTO bronze.raw_records (
                source_file, source_row_number, source_record_id,
                file_sha256, payload, pipeline_run_id
            )
            SELECT
                %s,
                src.row_number::INTEGER,
                COALESCE(
                    src.payload ->> 'source_row_id',
                    src.payload ->> 'event_id',
                    src.payload ->> 'city_id'
                ),
                %s,
                src.payload,
                %s
            FROM jsonb_array_elements(%s::jsonb)
            WITH ORDINALITY AS src(payload, row_number)
            ON CONFLICT (source_file, file_sha256, source_row_number)
            DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) FROM inserted
    """
    total_inserted = 0
    with connect("local") as connection:
        # Serialize cooperative ingestion runs; the primary key also guards reruns.
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext('omnichannel_bronze_ingestion'))"
        )
        for source in sources:
            cursor = connection.execute(query, (
                source["source_file"], source["file_sha256"], run_id,
                as_json(source["records"]),
            ))
            inserted_count = cursor.fetchone()[0]
            total_inserted += inserted_count
            source["loaded_at"] = None
            if inserted_count > 0:
                source["loaded_at"] = connection.execute(
                    "SELECT clock_timestamp()"
                ).fetchone()[0]
            print(f"[LOAD] {source['source_file']} | record_baru={inserted_count}")

    print(f"Total record baru: {total_inserted}")
    return total_inserted


def save_quality_result(run_id, source, bronze_count, mismatch_count):
    expected_count = len(source["records"])
    passed = bronze_count == expected_count and mismatch_count == 0
    status = "PASS" if passed else "FAIL"
    details = {
        "actual_value": bronze_count,
        "target_value": expected_count,
        "mismatch_count": mismatch_count,
        "file_sha256": source["file_sha256"],
        "unit": "records",
    }
    # Commit evidence of failed checks separately from the read-only verifier.
    with connect("local") as connection:
        connection.execute("""
            INSERT INTO ops.quality_results (
                pipeline_run_id, stage, check_name,
                status, failed_record_count, details
            )
            VALUES (%s, 'bronze', %s, %s, %s, %s::jsonb)
            ON CONFLICT (pipeline_run_id, stage, check_name)
            DO UPDATE SET
                status = EXCLUDED.status,
                failed_record_count = EXCLUDED.failed_record_count,
                details = EXCLUDED.details,
                checked_at = CURRENT_TIMESTAMP
        """, (
            run_id, f"source_match:{source['source_file']}",
            status, mismatch_count, as_json(details),
        ))
    return passed, status


def verify_bronze(sources, run_id):
    query = """
        WITH expected AS (
            SELECT ordinality AS row_number, value AS payload
            FROM jsonb_array_elements(%s::jsonb) WITH ORDINALITY
        ),
        existing AS (
            SELECT source_row_number, payload
            FROM bronze.raw_records
            WHERE source_file = %s AND file_sha256 = %s
        )
        SELECT
            (SELECT COUNT(*) FROM existing) AS bronze_count,
            COUNT(*) FILTER (
                WHERE expected.row_number IS NULL
                   OR existing.source_row_number IS NULL
                   OR expected.payload IS DISTINCT FROM existing.payload
            ) AS mismatch_count
        FROM expected
        FULL JOIN existing
            ON expected.row_number = existing.source_row_number
    """
    with connect("local") as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        for source in sources:
            cursor = connection.execute(query, (
                as_json(source["records"]),
                source["source_file"], source["file_sha256"],
            ))
            bronze_count, mismatch_count = cursor.fetchone()
            passed, status = save_quality_result(
                run_id, source, bronze_count, mismatch_count
            )
            print(
                f"[{status}] {source['source_file']} | "
                f"raw={len(source['records'])} | bronze={bronze_count} | "
                f"mismatch={mismatch_count}"
            )
            if not passed:
                raise ValueError(f"Verifikasi gagal: {source['source_file']}")

    print("Semua file sumber cocok dengan Bronze.")


def register_files(sources, run_id):
    """Register verified versions; keep unknown historical load times NULL.

    pipeline_run_id identifies the registration run. The original ingestion
    run on historical Bronze rows is preserved and may have no surviving log.
    registered_at is set automatically by PostgreSQL when metadata is created.
    """
    with connect("local") as connection:
        for source in sources:
            connection.execute("""
                INSERT INTO ops.ingested_files (
                    pipeline_run_id, source_file, file_checksum,
                    file_size_bytes, record_count, loaded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_file, file_checksum) DO NOTHING
            """, (
                run_id, source["source_file"], source["file_sha256"],
                source["file_size_bytes"], len(source["records"]),
                source.get("loaded_at"),
            ))
            # Existing metadata must agree with the same immutable file version.
            stored = connection.execute("""
                SELECT file_size_bytes, record_count
                FROM ops.ingested_files
                WHERE source_file = %s AND file_checksum = %s
            """, (source["source_file"], source["file_sha256"])).fetchone()
            if stored is None or tuple(stored) != (
                source["file_size_bytes"], len(source["records"])
            ):
                raise ValueError(f"Metadata file tidak cocok: {source['source_file']}")

    print(f"Informasi {len(sources)} file tercatat di Ops.")


def build_bronze(run_id, raw_dir=None):
    """Callable for a future DAG; its run must already exist in ops.pipeline_runs."""
    sources = validate_raw(
        raw_dir if raw_dir is not None else PROJECT_ROOT / "data" / "raw"
    )
    total_records = sum(len(source["records"]) for source in sources)
    print(f"Total file: {len(sources)}")
    print(f"Total record: {total_records}")

    inserted = load_bronze(sources, run_id)
    verify_bronze(sources, run_id)
    register_files(sources, run_id)

    # Preserve the selected file versions even when reruns append zero records.
    summary = {
        "file_count": len(sources),
        "record_count": total_records,
        "inserted_records": inserted,
        "input_files": [{
            "source_file": source["source_file"],
            "file_sha256": source["file_sha256"],
            "record_count": len(source["records"]),
        } for source in sources],
    }
    with connect("local") as connection:
        connection.execute("""
            UPDATE ops.pipeline_runs
            SET metadata = metadata || %s::jsonb
            WHERE pipeline_run_id = %s
        """, (as_json({"bronze": summary}), run_id))
    return summary


def main():
    initialize_schemas()
    run_id = str(uuid4())
    with connect("local") as connection:
        connection.execute("""
            INSERT INTO ops.pipeline_runs (
                pipeline_run_id, pipeline_name, status, metadata
            )
            VALUES (%s, 'bronze_ingestion', 'RUNNING', %s::jsonb)
        """, (run_id, as_json({"operation": "load_and_verify_bronze"})))

    print(f"Run ID: {run_id}")
    try:
        build_bronze(run_id)
        with connect("local") as connection:
            connection.execute("""
                UPDATE ops.pipeline_runs
                SET status = 'SUCCESS', finished_at = CURRENT_TIMESTAMP
                WHERE pipeline_run_id = %s
            """, (run_id,))
        print("Status Bronze tercatat di Ops: SUCCESS")
    except Exception as error:
        with connect("local") as connection:
            connection.execute("""
                UPDATE ops.pipeline_runs
                SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE pipeline_run_id = %s
            """, (str(error), run_id))
        raise


if __name__ == "__main__":
    main()
