package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// BenchmarkResult holds the outcome of a single benchmark run.
type BenchmarkResult struct {
	Name       string
	Operations int
	Duration   time.Duration
	Latencies  []time.Duration
	Errors     int64
}

// LatencyStats computes percentile latency statistics from recorded latencies.
type LatencyStats struct {
	Min time.Duration
	Max time.Duration
	Avg time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
}

func (r *BenchmarkResult) Stats() LatencyStats {
	if len(r.Latencies) == 0 {
		return LatencyStats{}
	}

	sorted := make([]time.Duration, len(r.Latencies))
	copy(sorted, r.Latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, l := range sorted {
		total += l
	}

	return LatencyStats{
		Min: sorted[0],
		Max: sorted[len(sorted)-1],
		Avg: total / time.Duration(len(sorted)),
		P50: sorted[percentileIndex(len(sorted), 50)],
		P95: sorted[percentileIndex(len(sorted), 95)],
		P99: sorted[percentileIndex(len(sorted), 99)],
	}
}

func (r *BenchmarkResult) OpsPerSec() float64 {
	if r.Duration == 0 {
		return 0
	}
	return float64(r.Operations) / r.Duration.Seconds()
}

func percentileIndex(n int, pct int) int {
	idx := int(math.Ceil(float64(pct)/100.0*float64(n))) - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

// Config holds the CLI configuration.
type Config struct {
	ConnString   string
	Database     string
	Collection   string
	NumOps       int
	Concurrency  int
	DocSizeBytes int
	Benchmarks   string
	CleanupFirst bool
	NoIndexes    bool
}

func parseConfig() Config {
	cfg := Config{}
	flag.StringVar(&cfg.ConnString, "conn", "mongodb://localhost:27017", "MongoDB connection string")
	flag.StringVar(&cfg.Database, "db", "bench_db", "Database name")
	flag.StringVar(&cfg.Collection, "coll", "bench_coll", "Collection name")
	flag.IntVar(&cfg.NumOps, "ops", 10000, "Number of operations per benchmark")
	flag.IntVar(&cfg.Concurrency, "concurrency", 1, "Number of concurrent workers")
	flag.IntVar(&cfg.DocSizeBytes, "docsize", 256, "Approximate document payload size in bytes")
	flag.StringVar(&cfg.Benchmarks, "benchmarks", "all", "Comma-separated list of benchmarks to run: insert,get,find_filter,find_sort (or 'all')")
	flag.BoolVar(&cfg.CleanupFirst, "cleanup", true, "Drop collection before running benchmarks")
	flag.BoolVar(&cfg.NoIndexes, "no-indexes", false, "Skip index creation for find benchmarks")
	flag.Parse()
	return cfg
}

func main() {
	cfg := parseConfig()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("MDB Bench - MongoDB Benchmark Tool")
	log.Printf("===================================")
	log.Printf("Connection:  %s", cfg.ConnString)
	log.Printf("Database:    %s", cfg.Database)
	log.Printf("Collection:  %s", cfg.Collection)
	log.Printf("Operations:  %d", cfg.NumOps)
	log.Printf("Concurrency: %d", cfg.Concurrency)
	log.Printf("Doc Size:    ~%d bytes", cfg.DocSizeBytes)
	log.Printf("")

	ctx := context.Background()

	// Connect to MongoDB.
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.ConnString))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Printf("Warning: disconnect error: %v", err)
		}
	}()

	// Verify connectivity.
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}
	log.Printf("Connected successfully.")

	coll := client.Database(cfg.Database).Collection(cfg.Collection)

	if cfg.CleanupFirst {
		log.Printf("Dropping collection %s.%s...", cfg.Database, cfg.Collection)
		_ = coll.Drop(ctx)
	}

	// Create indexes to support find queries.
	if !cfg.NoIndexes {
		ensureIndexes(ctx, coll)
	}

	// Determine which benchmarks to run.
	benchmarks := parseBenchmarkList(cfg.Benchmarks)

	var results []BenchmarkResult

	if benchmarks["insert"] {
		r := runInsertBenchmark(ctx, coll, cfg)
		results = append(results, r)
	}

	if benchmarks["get"] {
		r := runGetBenchmark(ctx, coll, cfg)
		results = append(results, r)
	}

	if benchmarks["find_filter"] {
		r := runFindFilterBenchmark(ctx, coll, cfg)
		results = append(results, r)
	}

	if benchmarks["find_sort"] {
		r := runFindSortBenchmark(ctx, coll, cfg)
		results = append(results, r)
	}

	// Print summary.
	printResults(results)
}

func parseBenchmarkList(input string) map[string]bool {
	all := map[string]bool{
		"insert":      false,
		"get":         false,
		"find_filter": false,
		"find_sort":   false,
	}
	if input == "all" {
		for k := range all {
			all[k] = true
		}
		return all
	}
	for _, b := range strings.Split(input, ",") {
		b = strings.TrimSpace(b)
		if _, ok := all[b]; ok {
			all[b] = true
		} else {
			log.Fatalf("Unknown benchmark: %q (valid: insert, get, find_filter, find_sort)", b)
		}
	}
	return all
}

// generatePayload creates a random string payload of approximately the given size.
func generatePayload(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func docID(i int) string {
	return fmt.Sprintf("bench-doc-%08d", i)
}

// --------------------------------------------------------------------------
// Benchmark: Insert (Set)
// --------------------------------------------------------------------------

func runInsertBenchmark(ctx context.Context, coll *mongo.Collection, cfg Config) BenchmarkResult {
	log.Printf("[INSERT] Starting %d insert operations with concurrency %d...", cfg.NumOps, cfg.Concurrency)

	latencies := make([]time.Duration, cfg.NumOps)
	var errCount int64

	start := time.Now()
	runConcurrent(cfg.NumOps, cfg.Concurrency, func(i int) {
		doc := bson.D{
			{Key: "_id", Value: docID(i)},
			{Key: "seq", Value: i},
			{Key: "category", Value: fmt.Sprintf("cat-%d", i%100)},
			{Key: "score", Value: rand.Float64() * 1000},
			{Key: "payload", Value: generatePayload(cfg.DocSizeBytes)},
			{Key: "tags", Value: bson.A{
				fmt.Sprintf("tag-%d", i%10),
				fmt.Sprintf("tag-%d", (i+1)%10),
			}},
			{Key: "created_at", Value: time.Now()},
		}

		opStart := time.Now()
		_, err := coll.InsertOne(ctx, doc)
		latencies[i] = time.Since(opStart)

		if err != nil {
			if n := atomic.AddInt64(&errCount, 1); n <= 10 {
				log.Printf("[INSERT] Error (op %d): %v", i, err)
			}
		}
	})
	elapsed := time.Since(start)

	result := BenchmarkResult{
		Name:       "Insert (Set)",
		Operations: cfg.NumOps,
		Duration:   elapsed,
		Latencies:  latencies,
		Errors:     errCount,
	}
	log.Printf("[INSERT] Completed in %v (%.0f ops/sec, %d errors)", elapsed, result.OpsPerSec(), errCount)
	return result
}

// --------------------------------------------------------------------------
// Benchmark: FindOne by _id (Get)
// --------------------------------------------------------------------------

func runGetBenchmark(ctx context.Context, coll *mongo.Collection, cfg Config) BenchmarkResult {
	// Ensure documents exist from a prior insert benchmark or seed them.
	ensureDocuments(ctx, coll, cfg)

	log.Printf("[GET] Starting %d findOne-by-_id operations with concurrency %d...", cfg.NumOps, cfg.Concurrency)

	latencies := make([]time.Duration, cfg.NumOps)
	var errCount int64

	start := time.Now()
	runConcurrent(cfg.NumOps, cfg.Concurrency, func(i int) {
		id := docID(rand.Intn(cfg.NumOps))
		filter := bson.D{{Key: "_id", Value: id}}

		opStart := time.Now()
		var result bson.M
		err := coll.FindOne(ctx, filter).Decode(&result)
		latencies[i] = time.Since(opStart)

		if err != nil {
			if n := atomic.AddInt64(&errCount, 1); n <= 10 {
				log.Printf("[GET] Error (op %d, id %s): %v", i, id, err)
			}
		}
	})
	elapsed := time.Since(start)

	r := BenchmarkResult{
		Name:       "FindOne by _id (Get)",
		Operations: cfg.NumOps,
		Duration:   elapsed,
		Latencies:  latencies,
		Errors:     errCount,
	}
	log.Printf("[GET] Completed in %v (%.0f ops/sec, %d errors)", elapsed, r.OpsPerSec(), errCount)
	return r
}

// --------------------------------------------------------------------------
// Benchmark: Find with filter
// --------------------------------------------------------------------------

func runFindFilterBenchmark(ctx context.Context, coll *mongo.Collection, cfg Config) BenchmarkResult {
	ensureDocuments(ctx, coll, cfg)

	// For find benchmarks we use fewer iterations since each is heavier.
	numOps := cfg.NumOps / 10
	if numOps < 100 {
		numOps = 100
	}

	log.Printf("[FIND_FILTER] Starting %d find-with-filter operations with concurrency %d...", numOps, cfg.Concurrency)

	latencies := make([]time.Duration, numOps)
	var errCount int64

	start := time.Now()
	runConcurrent(numOps, cfg.Concurrency, func(i int) {
		// Query by category field to simulate a filtered scan.
		cat := fmt.Sprintf("cat-%d", rand.Intn(100))
		filter := bson.D{{Key: "category", Value: cat}}

		opStart := time.Now()
		cursor, err := coll.Find(ctx, filter)
		if err != nil {
			if n := atomic.AddInt64(&errCount, 1); n <= 10 {
				log.Printf("[FIND_FILTER] Find error (op %d): %v", i, err)
			}
			latencies[i] = time.Since(opStart)
			return
		}

		// Drain the cursor to measure full round-trip.
		var docs []bson.M
		if err := cursor.All(ctx, &docs); err != nil {
			if n := atomic.AddInt64(&errCount, 1); n <= 10 {
				log.Printf("[FIND_FILTER] Cursor error (op %d): %v", i, err)
			}
		}
		latencies[i] = time.Since(opStart)
	})
	elapsed := time.Since(start)

	r := BenchmarkResult{
		Name:       "Find with Filter",
		Operations: numOps,
		Duration:   elapsed,
		Latencies:  latencies,
		Errors:     errCount,
	}
	log.Printf("[FIND_FILTER] Completed in %v (%.0f ops/sec, %d errors)", elapsed, r.OpsPerSec(), errCount)
	return r
}

// --------------------------------------------------------------------------
// Benchmark: Find with sort + limit
// --------------------------------------------------------------------------

func runFindSortBenchmark(ctx context.Context, coll *mongo.Collection, cfg Config) BenchmarkResult {
	ensureDocuments(ctx, coll, cfg)

	numOps := cfg.NumOps / 10
	if numOps < 100 {
		numOps = 100
	}

	log.Printf("[FIND_SORT] Starting %d find-with-sort-limit operations with concurrency %d...", numOps, cfg.Concurrency)

	latencies := make([]time.Duration, numOps)
	var errCount int64

	start := time.Now()
	runConcurrent(numOps, cfg.Concurrency, func(i int) {
		// Find top 10 documents by score in a given category.
		cat := fmt.Sprintf("cat-%d", rand.Intn(100))
		filter := bson.D{{Key: "category", Value: cat}}
		opts := options.Find().
			SetSort(bson.D{{Key: "score", Value: -1}}).
			SetLimit(10)

		opStart := time.Now()
		cursor, err := coll.Find(ctx, filter, opts)
		if err != nil {
			if n := atomic.AddInt64(&errCount, 1); n <= 10 {
				log.Printf("[FIND_SORT] Find error (op %d): %v", i, err)
			}
			latencies[i] = time.Since(opStart)
			return
		}

		var docs []bson.M
		if err := cursor.All(ctx, &docs); err != nil {
			if n := atomic.AddInt64(&errCount, 1); n <= 10 {
				log.Printf("[FIND_SORT] Cursor error (op %d): %v", i, err)
			}
		}
		latencies[i] = time.Since(opStart)
	})
	elapsed := time.Since(start)

	r := BenchmarkResult{
		Name:       "Find with Sort+Limit",
		Operations: numOps,
		Duration:   elapsed,
		Latencies:  latencies,
		Errors:     errCount,
	}
	log.Printf("[FIND_SORT] Completed in %v (%.0f ops/sec, %d errors)", elapsed, r.OpsPerSec(), errCount)
	return r
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// ensureIndexes creates indexes needed to support the find benchmarks.
func ensureIndexes(ctx context.Context, coll *mongo.Collection) {
	log.Printf("Creating indexes...")

	indexes := []mongo.IndexModel{
		{
			// Supports find_filter benchmark (filter by category).
			Keys: bson.D{{Key: "category", Value: 1}},
		},
		{
			// Supports find_sort benchmark (filter by category, sort by score desc).
			Keys: bson.D{
				{Key: "category", Value: 1},
				{Key: "score", Value: -1},
			},
		},
	}

	names, err := coll.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		log.Fatalf("Failed to create indexes: %v", err)
	}
	log.Printf("Created indexes: %v", names)
}

// ensureDocuments checks if the collection has enough documents, seeding if needed.
func ensureDocuments(ctx context.Context, coll *mongo.Collection, cfg Config) {
	count, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil || count < int64(cfg.NumOps) {
		log.Printf("  Seeding %d documents for benchmark...", cfg.NumOps)
		seedDocuments(ctx, coll, cfg)
	}
}

func seedDocuments(ctx context.Context, coll *mongo.Collection, cfg Config) {
	const batchSize = 500
	for i := 0; i < cfg.NumOps; i += batchSize {
		end := i + batchSize
		if end > cfg.NumOps {
			end = cfg.NumOps
		}

		var docs []interface{}
		for j := i; j < end; j++ {
			docs = append(docs, bson.D{
				{Key: "_id", Value: docID(j)},
				{Key: "seq", Value: j},
				{Key: "category", Value: fmt.Sprintf("cat-%d", j%100)},
				{Key: "score", Value: rand.Float64() * 1000},
				{Key: "payload", Value: generatePayload(cfg.DocSizeBytes)},
				{Key: "tags", Value: bson.A{
					fmt.Sprintf("tag-%d", j%10),
					fmt.Sprintf("tag-%d", (j+1)%10),
				}},
				{Key: "created_at", Value: time.Now()},
			})
		}

		_, err := coll.InsertMany(ctx, docs)
		if err != nil {
			// Documents may already exist if we're re-seeding.
			// Ignore duplicate key errors and continue.
			if !mongo.IsDuplicateKeyError(err) {
				log.Printf("  Warning: seed batch insert error: %v", err)
			}
		}
	}
}

// runConcurrent distributes 'total' operations across 'concurrency' goroutines.
func runConcurrent(total int, concurrency int, fn func(i int)) {
	if concurrency <= 1 {
		for i := 0; i < total; i++ {
			fn(i)
		}
		return
	}

	var wg sync.WaitGroup
	work := make(chan int, concurrency*2)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				fn(i)
			}
		}()
	}

	for i := 0; i < total; i++ {
		work <- i
	}
	close(work)
	wg.Wait()
}

// printResults outputs a formatted summary table.
func printResults(results []BenchmarkResult) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                                      BENCHMARK RESULTS                                                 ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "  BENCHMARK\tOPS\tDURATION\tOPS/SEC\tERRORS\tMIN\tAVG\tP50\tP95\tP99\tMAX\t\n")
	fmt.Fprintf(w, "  ---------\t---\t--------\t-------\t------\t---\t---\t---\t---\t---\t---\t\n")

	for _, r := range results {
		s := r.Stats()
		fmt.Fprintf(w, "  %s\t%d\t%v\t%.0f\t%d\t%v\t%v\t%v\t%v\t%v\t%v\t\n",
			r.Name,
			r.Operations,
			r.Duration.Round(time.Millisecond),
			r.OpsPerSec(),
			r.Errors,
			s.Min.Round(time.Microsecond),
			s.Avg.Round(time.Microsecond),
			s.P50.Round(time.Microsecond),
			s.P95.Round(time.Microsecond),
			s.P99.Round(time.Microsecond),
			s.Max.Round(time.Microsecond),
		)
	}
	w.Flush()
	fmt.Println()
}
