# DeltaQuery

DeltaQuery is an open-source SQL query engine to support data lakehouse architecture based on DeltaLake. It depends on various, powerful open-source engines for bigdata, such as [Arrow](https://arrow.apache.org/), [Delta](https://delta.io/), [Duckdb](https://duckdb.org/), and so on.

# Building

DeltaQuery is compiled using [Cargo](https://doc.rust-lang.org/cargo/).

To compile, run

```bash
cargo build
```

To execute sample [Minio](https://min.io/) and [Trino](https://trino.io/) servers using [Docker](https://www.docker.com/), run in the samples directory

```bash
docker-compose up
```

To execute server, run

```bash
cargo run -- --logfilter deltasync=info --config ./samples/configs/delta.yaml
```

To execute python client using adbc, run

```bash
python3 ./examples/adbc_flightsql_query.py --server "grpc://localhost:32010" --authorization "Basic YWRtaW46YWRtaW4K" --query "select date,hour,name,score from delta.default.test0"
```

To execute rust client using flight sql, run

```bash
cargo run -p flight_sql -- --logfilter flight_sql=info --command statement-query --host 127.0.0.1 --port 32010 --protocol http --authorization "Basic YWRtaW46YWRtaW4K" --query "select date,hour,name,score from delta.default.test0" --output pretty
```

# License

Apache License 2.0, see [LICENSE](https://github.com/ingkle/deltaquery/blob/master/LICENSE).
