import argparse
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", help="server", default="grpc://localhost:32010")
    parser.add_argument(
        "--authorization", help="authorization", default="Basic YWRtaW46YWRtaW4K"
    )
    parser.add_argument("--database", help="database", default="postgres")
    parser.add_argument(
        "--query",
        help="query",
        default="select date,hour,name,score from delta.default.test0",
    )
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
            cur.execute(args.query)
            table = cur.fetch_arrow_table()
            print(table.to_pandas())
