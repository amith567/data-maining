import csv
import re
import hashlib
from pathlib import Path
from collections import defaultdict
import duckdb

# Source sales folder
SALES = Path.home() / "Desktop" / "data_2" / "data" / "sales"

# Output folder
OUT = Path.home() / "Desktop" / "annapurna_exam" / "output" / "sales"
OUT.mkdir(parents=True, exist_ok=True)

# Store unique sales lines
rows = {}

print("Reading sales files...")
print("Source:", SALES)

for path in SALES.glob("*.csv"):

    name = path.name

    # Read store and business date from filename
    m = re.match(r"SALES_(S\d+)_(\d{8})(?:__R\d+)?\.csv$", name)

    if not m:
        continue

    store_id = m.group(1)
    business_date = m.group(2)

    # Detect delimiter
    with open(path, "r", encoding="utf-8-sig",
              errors="replace", newline="") as f:
        header = f.readline()

    delimiter = ";" if ";" in header else ","

    con = duckdb.connect()

    query = f"""
        SELECT *
        FROM read_csv(
            '{path.as_posix().replace("'", "''")}',
            delim='{delimiter}',
            header=true,
            auto_detect=true,
            ignore_errors=false
        )
    """

    try:
        data = con.execute(query).fetchall()
        columns = [d[0] for d in con.description]
        con.close()
    except Exception as e:
        con.close()
        print("ERROR reading:", name)
        print(e)
        continue

    # Prefer original file over re-send files
    is_resend = "__R" in name

    for r in data:

        d = dict(zip(columns, r))

        bill_no = str(d["bill_no"])
        line_no = int(d["line_no"])

        product_code = d.get("product_code")
        if product_code is None:
            product_code = d.get("item_code")

        qty = d.get("qty")
        if qty is None:
            qty = d.get("quantity")

        unit_price = d.get("unit_price")
        if unit_price is None:
            unit_price = d.get("rate")

        line_type = d.get("line_type")
        if line_type is None:
            line_type = d.get("type")

        ts = d.get("ts")
        if ts is None:
            ts = d.get("txn_time")

        key = (bill_no, line_no)

        new_row = (
            bill_no,
            line_no,
            str(product_code),
            float(qty),
            float(unit_price),
            str(line_type),
            str(ts),
            store_id,
            business_date
        )

        # Original files get priority over re-send files
        if key not in rows:
            rows[key] = (is_resend, new_row)
        else:
            old_is_resend, old_row = rows[key]

            if old_is_resend and not is_resend:
                rows[key] = (is_resend, new_row)

print("Unique rows:", len(rows))

# Remove priority flag
final_rows = [value[1] for value in rows.values()]

# Group by year/month/store
by_partition = defaultdict(list)

for row in final_rows:

    business_date = row[8]
    store_id = row[7]

    year = business_date[:4]
    month = business_date[4:6]

    by_partition[(year, month, store_id)].append(row)

# Remove old parquet files
for p in OUT.rglob("*.parquet"):
    p.unlink()

print("Creating partitioned Parquet files...")

con = duckdb.connect()

for (year, month, store_id), data in sorted(by_partition.items()):

    directory = (
        OUT
        / f"year={year}"
        / f"month={month}"
        / f"store={store_id}"
    )

    directory.mkdir(parents=True, exist_ok=True)

    temp = OUT / "_temp.csv"

    with open(temp, "w", encoding="utf-8", newline="") as f:

        writer = csv.writer(f)

        writer.writerow([
            "bill_no",
            "line_no",
            "product_code",
            "qty",
            "unit_price",
            "line_type",
            "ts",
            "store_id",
            "business_date"
        ])

        writer.writerows(data)

    output = directory / "sales.parquet"

    con.execute(f"""
        COPY (
            SELECT
                bill_no,
                line_no,
                product_code,
                CAST(qty AS DOUBLE) AS qty,
                CAST(unit_price AS DECIMAL(18,2)) AS unit_price,
                line_type,
                ts,
                store_id,
                business_date
            FROM read_csv_auto('{temp.as_posix()}')
        )
        TO '{output.as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    temp.unlink()

con.close()

# Calculate row count and checksum
sha = hashlib.sha256()
count = 0

for p in sorted(OUT.rglob("*.parquet")):

    with open(p, "rb") as f:

        while True:
            block = f.read(1024 * 1024)

            if not block:
                break

            sha.update(block)

    con = duckdb.connect()

    count += con.execute(
        f"SELECT count(*) FROM read_parquet('{p.as_posix()}')"
    ).fetchone()[0]

    con.close()

print()
print("===================================")
print("LOAD COMPLETE")
print("===================================")
print("Canonical row count:", count)
print("Dataset checksum:", sha.hexdigest())
print(
    "Partition files:",
    len(list(OUT.rglob("*.parquet")))
)
print("Output:", OUT)