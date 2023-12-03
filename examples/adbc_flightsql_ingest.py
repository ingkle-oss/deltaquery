import argparse
import pyarrow
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", help="server", default="grpc://localhost:32010")
    parser.add_argument(
        "--authorization", help="authorization", default="Basic YWRtaW46YWRtaW4K"
    )
    parser.add_argument("--database", help="database", default="postgres")
    parser.add_argument("--table", help="table", default="delta.default.test0")
    parser.add_argument("--mode", help="mode", default="append")
    args = parser.parse_args()

    with connect(
        args.server,
        db_kwargs={
            DatabaseOptions.AUTHORIZATION_HEADER.value: args.authorization,
            DatabaseOptions.WITH_MAX_MSG_SIZE.value: "1073741824",
        },
        conn_kwargs={
            "adbc.flight.sql.rpc.call_header.x-flight-sql-database": args.database
        },
    ) as conn:
        with conn.cursor() as cur:
            table = pyarrow.table(
                [
                    ["2023-10-20", "2023-10-20", "2023-10-21"],
                    ["10", "11", "10"],
                    ["apple", "mango", "orange"],
                    [90, 100, 80],
                ],
                names=["date", "hour", "name", "score"],
            )
            res = cur.adbc_ingest(args.table, table, mode=args.mode)
            print(res)
