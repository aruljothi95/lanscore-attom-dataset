"""
ATTOM Data Pipeline Script
==========================
Reads DB credentials from .env file and executes the full
ATTOM data pipeline:
  1. Backup tables before deletes
  2. Apply property_deletes and recorder_deletes
  3. Verify crosswalk coverage
  4. Create final property tables (property_tax, property_loan,
     property_foreclosure, property_recorder, property_assignment_release)

Usage:
    pip install psycopg2-binary python-dotenv
    python attom_pipeline.py
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv

# ── Load .env ────────────────────────────────────────────────────────────────
# Looks for .env in the backend folder (adjust path if needed)
env_path = os.path.join(os.path.dirname(__file__), "backend", ".env")
if not os.path.exists(env_path):
    # fallback: same directory as this script
    env_path = os.path.join(os.path.dirname(__file__), ".env")

load_dotenv(dotenv_path=env_path)

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "landscor_live")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    print("ERROR: Missing DB credentials in .env file.")
    print("Required: DB_HOST, DB_USER, DB_PASSWORD (DB_NAME defaults to landscor_live)")
    sys.exit(1)


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def run_step(conn, description, sql, fetch_count=False):
    """Execute a SQL statement, print result and timing."""
    print(f"\n{'─'*60}")
    print(f"▶  {description}")
    print(f"{'─'*60}")
    start = time.time()
    with conn.cursor() as cur:
        cur.execute(sql)
        if fetch_count:
            row = cur.fetchone()
            count = row[0] if row else 0
            elapsed = time.time() - start
            print(f"   Count : {count:,}")
            print(f"   Time  : {elapsed:.1f}s")
            return count
        else:
            affected = cur.rowcount
            elapsed = time.time() - start
            print(f"   Rows affected : {affected:,}")
            print(f"   Time          : {elapsed:.1f}s")
            return affected


# ── SQL Definitions ───────────────────────────────────────────────────────────

STEPS = [

    # ── STEP 1: BACKUPS ───────────────────────────────────────────────────────
    {
        "description": "STEP 1a: Backup tax_assessor rows to be deleted",
        "sql": """
            CREATE TABLE IF NOT EXISTS attom_dataset.tax_assessor_delete_backup AS
            SELECT * FROM attom_dataset.tax_assessor
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 1b: Backup foreclosure rows to be deleted",
        "sql": """
            CREATE TABLE IF NOT EXISTS attom_dataset.foreclosure_delete_backup AS
            SELECT * FROM attom_dataset.foreclosure
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 1c: Backup loan_originator rows to be deleted",
        "sql": """
            CREATE TABLE IF NOT EXISTS attom_dataset.loan_originator_delete_backup AS
            SELECT * FROM attom_dataset.loan_originator
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 1d: Backup assignment_release rows to be deleted",
        "sql": """
            CREATE TABLE IF NOT EXISTS attom_dataset.assignment_release_delete_backup AS
            SELECT * FROM attom_dataset.assignment_release
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 1e: Backup property_to_boundarymatch_parcel rows to be deleted",
        "sql": """
            CREATE TABLE IF NOT EXISTS attom_dataset.property_to_boundarymatch_parcel_delete_backup AS
            SELECT * FROM attom_dataset.property_to_boundarymatch_parcel
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 1f: Backup recorder rows to be deleted (via recorder_deletes)",
        "sql": """
            CREATE TABLE IF NOT EXISTS attom_dataset.recorder_delete_backup AS
            SELECT * FROM attom_dataset.recorder
            WHERE "TransactionID" IN (SELECT "TransactionID" FROM attom_dataset.recorder_deletes);
        """
    },

    # ── STEP 2: APPLY DELETES ─────────────────────────────────────────────────
    # tax_assessor and property_to_boundarymatch_parcel are intentionally skipped
    {
        "description": "STEP 2a: Delete from foreclosure (property_deletes)",
        "sql": """
            DELETE FROM attom_dataset.foreclosure
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 2b: Delete from loan_originator (property_deletes)",
        "sql": """
            DELETE FROM attom_dataset.loan_originator
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 2c: Delete from assignment_release (property_deletes)",
        "sql": """
            DELETE FROM attom_dataset.assignment_release
            WHERE attom_id IN (SELECT attom_id FROM attom_dataset.property_deletes);
        """
    },
    {
        "description": "STEP 2d: Delete from recorder (recorder_deletes via TransactionID)",
        "sql": """
            DELETE FROM attom_dataset.recorder
            WHERE "TransactionID" IN (SELECT "TransactionID" FROM attom_dataset.recorder_deletes);
        """
    },

    # ── STEP 3: VERIFY CROSSWALK COVERAGE ────────────────────────────────────
    {
        "description": "STEP 3a: Verify crosswalk → tax_assessor matched count (expect ~687,568)",
        "sql": """
            SELECT COUNT(*)
            FROM attom_dataset.property_to_boundarymatch_parcel p
            JOIN attom_dataset.tax_assessor t ON t.attom_id = p.attom_id;
        """,
        "fetch_count": True
    },
    {
        "description": "STEP 3b: Verify crosswalk → tax_assessor unmatched count (expect ~6,170)",
        "sql": """
            SELECT COUNT(*)
            FROM attom_dataset.property_to_boundarymatch_parcel p
            WHERE NOT EXISTS (
                SELECT 1 FROM attom_dataset.tax_assessor t
                WHERE t.attom_id = p.attom_id
            );
        """,
        "fetch_count": True
    },
    {
        "description": "STEP 3c: Verify Travis GeoJSON → crosswalk match (expect ~420,067)",
        "sql": """
            SELECT COUNT(*)
            FROM attom_dataset.parcel_polygons_travis t
            INNER JOIN attom_dataset.property_to_boundarymatch_parcel p
                ON p.geoid = t.feature_id;
        """,
        "fetch_count": True
    },
    {
        "description": "STEP 3d: Verify Williamson GeoJSON → crosswalk match (expect ~273,835)",
        "sql": """
            SELECT COUNT(*)
            FROM attom_dataset.parcel_polygons_williamson w
            INNER JOIN attom_dataset.property_to_boundarymatch_parcel p
                ON p.geoid = w.feature_id;
        """,
        "fetch_count": True
    },

    # ── STEP 4: CREATE FINAL PROPERTY TABLES ─────────────────────────────────
    {
        "description": "STEP 4a: Create property_tax (expect ~687,568 rows)",
        "sql": """
            CREATE TABLE attom_dataset.property_tax AS
            SELECT
                p.geoid,
                ta.*
            FROM attom_dataset.property_to_boundarymatch_parcel p
            INNER JOIN attom_dataset.tax_assessor ta ON ta.attom_id = p.attom_id
            WHERE p.geoid IN (
                SELECT feature_id FROM attom_dataset.parcel_polygons_travis
                UNION
                SELECT feature_id FROM attom_dataset.parcel_polygons_williamson
            );
        """
    },
    {
        "description": "STEP 4a: Verify property_tax row count",
        "sql": "SELECT COUNT(*) FROM attom_dataset.property_tax;",
        "fetch_count": True
    },
    {
        "description": "STEP 4b: Create property_loan (expect ~160,797 rows)",
        "sql": """
            CREATE TABLE attom_dataset.property_loan AS
            SELECT
                p.geoid,
                l.*
            FROM attom_dataset.property_to_boundarymatch_parcel p
            INNER JOIN attom_dataset.loan_originator l ON l.attom_id = p.attom_id
            WHERE p.geoid IN (
                SELECT feature_id FROM attom_dataset.parcel_polygons_travis
                UNION
                SELECT feature_id FROM attom_dataset.parcel_polygons_williamson
            );
        """
    },
    {
        "description": "STEP 4b: Verify property_loan row count",
        "sql": "SELECT COUNT(*) FROM attom_dataset.property_loan;",
        "fetch_count": True
    },
    {
        "description": "STEP 4c: Create property_foreclosure (expect ~74,314 rows)",
        "sql": """
            CREATE TABLE attom_dataset.property_foreclosure AS
            SELECT
                p.geoid,
                f.*
            FROM attom_dataset.property_to_boundarymatch_parcel p
            INNER JOIN attom_dataset.foreclosure f ON f.attom_id = p.attom_id
            WHERE p.geoid IN (
                SELECT feature_id FROM attom_dataset.parcel_polygons_travis
                UNION
                SELECT feature_id FROM attom_dataset.parcel_polygons_williamson
            );
        """
    },
    {
        "description": "STEP 4c: Verify property_foreclosure row count",
        "sql": "SELECT COUNT(*) FROM attom_dataset.property_foreclosure;",
        "fetch_count": True
    },
    {
        "description": "STEP 4d: Create property_recorder (expect ~954,563 rows)",
        "sql": """
            CREATE TABLE attom_dataset.property_recorder AS
            SELECT
                p.geoid,
                r.*
            FROM attom_dataset.property_to_boundarymatch_parcel p
            INNER JOIN attom_dataset.recorder r ON r.attom_id = p.attom_id
            WHERE p.geoid IN (
                SELECT feature_id FROM attom_dataset.parcel_polygons_travis
                UNION
                SELECT feature_id FROM attom_dataset.parcel_polygons_williamson
            );
        """
    },
    {
        "description": "STEP 4d: Verify property_recorder row count",
        "sql": "SELECT COUNT(*) FROM attom_dataset.property_recorder;",
        "fetch_count": True
    },
    {
        "description": "STEP 4e: Create property_assignment_release (expect ~892,273 rows)",
        "sql": """
            CREATE TABLE attom_dataset.property_assignment_release AS
            SELECT
                p.geoid,
                ar.*
            FROM attom_dataset.property_to_boundarymatch_parcel p
            INNER JOIN attom_dataset.assignment_release ar ON ar.attom_id = p.attom_id
            WHERE p.geoid IN (
                SELECT feature_id FROM attom_dataset.parcel_polygons_travis
                UNION
                SELECT feature_id FROM attom_dataset.parcel_polygons_williamson
            );
        """
    },
    {
        "description": "STEP 4e: Verify property_assignment_release row count",
        "sql": "SELECT COUNT(*) FROM attom_dataset.property_assignment_release;",
        "fetch_count": True
    },
]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  ATTOM DATA PIPELINE")
    print(f"  DB   : {DB_NAME} @ {DB_HOST}:{DB_PORT}")
    print(f"  User : {DB_USER}")
    print("=" * 60)

    conn = get_connection()
    conn.autocommit = True  # each step commits independently

    total_start = time.time()
    errors = []

    for i, step in enumerate(STEPS):
        try:
            fetch = step.get("fetch_count", False)
            run_step(conn, step["description"], step["sql"], fetch_count=fetch)
        except Exception as e:
            print(f"\n   ❌ ERROR: {e}")
            errors.append({"step": step["description"], "error": str(e)})
            # Continue to next step — don't abort entire pipeline
            continue

    conn.close()

    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE — Total time: {total_elapsed:.1f}s")
    if errors:
        print(f"\n  ⚠️  {len(errors)} step(s) had errors:")
        for e in errors:
            print(f"     - {e['step']}")
            print(f"       {e['error']}")
    else:
        print("  ✅ All steps completed successfully")
    print("=" * 60)


if __name__ == "__main__":
    main()