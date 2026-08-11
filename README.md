# MDB Bench

A lightweight MongoDB benchmarking tool written in Go. Designed to measure latency and throughput of different MongoDB-compatible server implementations.

## Benchmarks

| Benchmark | Operation | Description |
|---|---|---|
| `insert` | `InsertMany` | Inserts documents in batches of 50 with structured fields and a configurable payload size |
| `get` | `FindOne` by `_id` | Fetches a single document by primary key — exercises the KV fast-path |
| `find_filter` | `Find` with equality filter | Queries documents by a `category` field and drains the cursor |
| `find_sort` | `Find` with sort + limit | Queries by category, sorts by `score` descending, limits to 10 results |
| `range_scan_id` | Range scan with limit + Mixed update | Range scan on the `_id` field ($gt random ID) limited to 50 documents, with 5% pure update operations |
| `range_scan` | Range scan with limit + Mixed update | The same operation on the ordinary indexed field `a`, which holds the same value as `_id` in every document |

`range_scan_id` and `range_scan` are one benchmark over two fields that always
hold the same value, so the difference between them is the difference between
`_id` and an ordinary indexed field — a server may index `_id` specially, and
may serve a point lookup by `_id` without going through its query path at all.

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
| `-benchmarks` | `all` | Comma-separated list: `insert`, `get`, `find_filter`, `find_sort`, `range_scan_id`, `range_scan` |
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
      Insert (Batched)  10000     4.2s     2381       0    312µs    420µs    398µs    687µs   1.2ms   4.1ms
  FindOne by _id    10000     2.1s     4762       0    148µs    210µs    195µs    382µs    712µs   3.8ms
  Find with Filter   1000     3.8s      263       0    1.2ms    3.8ms    3.1ms    8.2ms   12.4ms  28.1ms
  Find Sort+Limit    1000     2.9s      345       0    980µs    2.9ms    2.4ms    6.8ms   10.1ms  22.3ms
```

## How It Works

1. **Insert benchmark** performs batched writes (in batches of 50 documents) containing an `_id`, `a`, `seq`, `category`, `score`, `payload`, `tags`, and `created_at` field. The `payload` is a random string of the configured size. `a` is a copy of `_id`, and exists so the two range-scan benchmarks walk identical data.

2. **Get benchmark** performs `FindOne` lookups using random `_id` values from the inserted document set. Since it depends on these documents, the **Insert benchmark** is automatically run first to seed the collection.

3. **Find benchmarks** query by the `category` field (100 distinct values across the dataset) and drain all results. The sort variant adds a descending sort on `score` with a limit of 10.

4. **Range scan benchmarks** perform a range scan using a `$gt` filter with a random document ID as the starting point, sorted ascending and limited to 50. In these benchmarks, 5% of operations executed are randomly chosen to be pure update operations on randomly selected documents (updating their score); the update addresses the same field the scan does. `range_scan_id` uses `_id`; `range_scan` uses `a`, which carries the same value and is indexed by `ensureIndexes` — without that index the benchmark would measure a collection scan rather than an indexed one.

5. **Concurrency** is implemented with a worker pool. Each worker pulls operation indices from a shared channel, ensuring even distribution.

6. **Automatic Seeding via Insert Benchmark** — if any of the read-dependent benchmarks (`get`, `find_filter`, `find_sort`, `range_scan_id`, or `range_scan`) are requested, the tool automatically enables and runs the `insert` benchmark first. This acts as the document seeding step, recording metrics for insertion performance as part of the overall results.

## Document Schema

Each benchmarked document has the following shape:

```json
{
  "_id": "bench-doc-00000042",
  "a": "bench-doc-00000042",
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
