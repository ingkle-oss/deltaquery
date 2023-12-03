import argparse
from adbc_driver_postgresql.dbapi import connect

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url", help="host", default="postgresql://admin:admin@localhost:5432/postgres"
    )
    parser.add_argument(
        "--query",
        help="query",
        default="select name,score from sample0",
    )
    args = parser.parse_args()

    with connect(args.url) as conn:
        with conn.cursor() as cur:
            cur.execute(args.query)
            table = cur.fetch_arrow_table()
            print(table.to_pandas())
