# MDB Bench

A lightweight MongoDB benchmarking tool written in Go. Designed to measure latency and throughput of different MongoDB-compatible server implementations.

## Benchmarks

| Benchmark | Operation | Description |
|---|---|---|
| `insert` | `InsertOne` | Inserts documents with structured fields and a configurable payload size |
| `get` | `FindOne` by `_id` | Fetches a single document by primary key — exercises the KV fast-path |
| `find_filter` | `Find` with equality filter | Queries documents by a `category` field and drains the cursor |
| `find_sort` | `Find` with sort + limit | Queries by category, sorts by `score` descending, limits to 10 results |

Each benchmark reports:
- **Throughput** — operations per second
- **Latency percentiles** — min, avg, p50, p95, p99, max
- **Error count**

## Installation

```bash
go install github.com/couchbaselabs/mdb-bench@latest
```

Or build from source:

```bash
git clone https://github.com/couchbaselabs/mdb-bench.git
cd mdb-bench
go build -o mdb-bench .
```

## Usage

```
mdb-bench [flags]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `-conn` | `mongodb://localhost:27017` | MongoDB connection string |
| `-db` | `bench_db` | Database name |
| `-coll` | `bench_coll` | Collection name |
| `-ops` | `10000` | Number of operations per benchmark |
| `-concurrency` | `1` | Number of concurrent workers |
| `-docsize` | `256` | Approximate document payload size in bytes |
| `-benchmarks` | `all` | Comma-separated list: `insert`, `get`, `find_filter`, `find_sort` |
| `-cleanup` | `true` | Drop collection before running benchmarks |

### Examples

Run all benchmarks against a local MongoDB instance:

```bash
mdb-bench -conn "mongodb://localhost:27017"
```

Run against another server with higher concurrency:

```bash
mdb-bench -conn "mongodb://localhost:27018" -ops 5000 -concurrency 4
```

Run only KV-style benchmarks (insert + get) with more operations:

```bash
mdb-bench -benchmarks insert,get -ops 50000 -concurrency 8
```

Benchmark with larger documents:

```bash
mdb-bench -docsize 4096 -ops 10000
```

Compare two implementations side by side:

```bash
# Terminal 1: Server A
mdb-bench -conn "mongodb://localhost:27017" -ops 20000 -concurrency 4

# Terminal 2: Server B
mdb-bench -conn "mongodb://localhost:27018" -ops 20000 -concurrency 4
```

### Sample Output

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                      BENCHMARK RESULTS                                                 ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════════════╝

         BENCHMARK    OPS  DURATION  OPS/SEC  ERRORS      MIN      AVG      P50      P95      P99      MAX
         ---------    ---  --------  -------  ------      ---      ---      ---      ---      ---      ---
      Insert (Set)  10000     4.2s     2381       0    312µs    420µs    398µs    687µs   1.2ms   4.1ms
  FindOne by _id    10000     2.1s     4762       0    148µs    210µs    195µs    382µs    712µs   3.8ms
  Find with Filter   1000     3.8s      263       0    1.2ms    3.8ms    3.1ms    8.2ms   12.4ms  28.1ms
  Find Sort+Limit    1000     2.9s      345       0    980µs    2.9ms    2.4ms    6.8ms   10.1ms  22.3ms
```

## How It Works

1. **Insert benchmark** writes documents containing an `_id`, `seq`, `category`, `score`, `payload`, `tags`, and `created_at` field. The `payload` is a random string of the configured size.

2. **Get benchmark** performs `FindOne` lookups using random `_id` values from the inserted document set. If documents don't exist yet, they are auto-seeded before the benchmark starts.

3. **Find benchmarks** query by the `category` field (100 distinct values across the dataset) and drain all results. The sort variant adds a descending sort on `score` with a limit of 10.

4. **Concurrency** is implemented with a worker pool. Each worker pulls operation indices from a shared channel, ensuring even distribution.

5. **Auto-seeding** — if the get or find benchmarks run without a prior insert, the tool automatically seeds the collection using batched `InsertMany` calls.

## Document Schema

Each benchmarked document has the following shape:

```json
{
  "_id": "bench-doc-00000042",
  "seq": 42,
  "category": "cat-42",
  "score": 723.156,
  "payload": "aB3x...",
  "tags": ["tag-2", "tag-3"],
  "created_at": "2026-04-20T18:30:00Z"
}
```

## License

Apache 2.0
