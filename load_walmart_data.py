from pathlib import Path
import psycopg2

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
DATA_DIR = BASE_DIR / "walmart_dataset" / "data"


def read_env_file(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise RuntimeError("Could not decode .env file with a supported encoding")


def extract_connection_string(env_text: str) -> str:
    for line in env_text.splitlines():
        line = line.strip()

        if line.startswith("DATABASE_URL="):
            connection_string = line.split("=", 1)[1].strip()
            connection_string = connection_string.strip('"').strip("'")

            if connection_string:
                return connection_string

    raise RuntimeError(
        "DATABASE_URL is missing from the .env file"
    )


def load_csv_to_table(conn, csv_path: Path, table_name: str):
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV HEADER",
                fh,
            )
    conn.commit()


if __name__ == "__main__":
    env_text = read_env_file(ENV_PATH)
    conn_str = extract_connection_string(env_text)

    with psycopg2.connect(conn_str) as conn:
        conn.autocommit = False
        files = [
            (DATA_DIR / "customers.csv", "raw.customers"),
            (DATA_DIR / "stores.csv", "raw.stores"),
            (DATA_DIR / "products.csv", "raw.products"),
            (DATA_DIR / "employees.csv", "raw.employees"),
            (DATA_DIR / "orders.csv", "raw.orders"),
            (DATA_DIR / "order_items.csv", "raw.order_items"),
        ]

        for csv_path, table_name in files:
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing CSV file: {csv_path}")
            load_csv_to_table(conn, csv_path, table_name)
            print(f"Loaded {csv_path.name} -> {table_name}")

        with conn.cursor() as cur:
            for _, table_name in files:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                print(f"{table_name}: {count} rows")
