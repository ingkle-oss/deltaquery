import argparse
from trino.dbapi import connect

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="trino host")
    parser.add_argument("--port", help="trino port")
    parser.add_argument("--table", help="table name")
    parser.add_argument("--query", help="target query")
    args = parser.parse_args()

    conn = connect(host=args.host, port=args.port, user="deltaquery", catalog="delta")
    cur = conn.cursor()

    cur.execute(args.query)
    for row in cur.fetchall():
        print(row)
