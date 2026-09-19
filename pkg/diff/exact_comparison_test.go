package diff

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jacktea/data-smith/pkg/chunk"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
)

func TestValueComparatorCoversDriverScalarShapes(t *testing.T) {
	numericValues := []any{
		int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		float32(1), float64(1), "1",
	}
	for _, value := range numericValues {
		if got := CompareValues(value, float64(2), "double precision"); got >= 0 {
			t.Errorf("CompareValues(%T(%v), 2) = %d, want < 0", value, value, got)
		}
	}

	for _, value := range []any{true, int(1), int8(1), int16(1), int32(1), int64(1), "yes"} {
		if !AreValuesEqual(value, true, "boolean") {
			t.Errorf("boolean driver value %T(%v) was not normalized", value, value)
		}
	}
	for _, value := range []any{false, int(0), "no"} {
		if !AreValuesEqual(value, false, "tinyint(1)") {
			t.Errorf("false driver value %T(%v) was not normalized", value, value)
		}
	}

	instant := time.Date(2026, time.September, 19, 1, 2, 3, 456000000, time.UTC)
	for _, value := range []any{instant, instant.Format(time.RFC3339Nano), "2026-09-19 01:02:03.456000"} {
		if !AreValuesEqual(value, instant, "timestamp") {
			t.Errorf("timestamp driver value %T(%v) was not normalized", value, value)
		}
	}

	if CompareValues([]byte{0x00, 0xff}, []byte{0x01}, "bytea") >= 0 {
		t.Fatal("binary values were not compared bytewise")
	}
	if !AreValuesEqual(math.Inf(1), "infinity", "double") {
		t.Fatal("positive infinity spellings should compare equal")
	}
}

func TestEqualJSONAcceptsDriverShapesAndRejectsInvalidValues(t *testing.T) {
	if !equalJSON([]byte(`{"a":1,"b":[true,null]}`), `{"b":[true,null],"a":1}`) {
		t.Fatal("JSON bytes and strings should compare structurally")
	}
	if !equalJSON(map[string]any{"a": float64(1)}, []byte(`{"a":1}`)) {
		t.Fatal("marshaled objects should compare with driver bytes")
	}
	if equalJSON(`{"a":`, `{"a":1}`) {
		t.Fatal("malformed JSON must not compare equal")
	}
	if equalJSON(make(chan int), map[string]any{"a": 1}) {
		t.Fatal("unmarshalable values must not compare equal")
	}
}

func TestValueComparatorNullBooleanStringAndBinaryOrdering(t *testing.T) {
	tests := []struct {
		name       string
		a, b       any
		columnType string
		want       int
	}{
		{"both null", nil, nil, "text", 0},
		{"null first", nil, "value", "text", -1},
		{"null last", "value", nil, "text", 1},
		{"string order", "alpha", "beta", "text", -1},
		{"boolean order", false, true, "boolean", -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CompareValues(test.a, test.b, test.columnType); got != test.want {
				t.Fatalf("CompareValues(%v, %v, %q) = %d, want %d", test.a, test.b, test.columnType, got, test.want)
			}
		})
	}
	if !AreValuesEqual([]byte{0, 1}, []byte{0, 1}, "bytea") {
		t.Fatal("equal binary values should compare equal")
	}
	if AreValuesEqual([]byte{0, 1}, []byte{0, 2}, "blob") {
		t.Fatal("different binary values should not compare equal")
	}
}

func TestValueComparatorExactNumericSemantics(t *testing.T) {
	tests := []struct {
		name    string
		a, b    any
		colType string
		wantCmp int
		wantEq  bool
	}{
		{"bigint beyond 2^53", "9007199254740992", "9007199254740993", "bigint", -1, false},
		{"unsigned max", uint64(math.MaxUint64), uint64(math.MaxUint64 - 1), "bigint unsigned", 1, false},
		{"decimal trailing zero", []byte("12345678901234567890.1234500"), []byte("12345678901234567890.12345"), "numeric", 0, true},
		{"decimal adjacent precision", "12345678901234567890.12345678901234567890", "12345678901234567890.12345678901234567891", "decimal(38,20)", -1, false},
		{"decimal has no float tolerance", "1.0000000000", "1.0000000005", "decimal", -1, false},
		{"float tolerance", 1.0, 1.0000000005, "double precision", 0, true},
		{"float NaN", math.NaN(), math.NaN(), "double precision", 0, true},
		{"float positive infinity", math.Inf(1), math.Inf(1), "float8", 0, true},
		{"float negative before positive infinity", math.Inf(-1), math.Inf(1), "float8", -1, false},
		{"numeric NaN", "NaN", "NaN", "numeric", 0, true},
		{"numeric infinity aliases", "Infinity", "+Inf", "numeric", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareValues(tt.a, tt.b, tt.colType); got != tt.wantCmp {
				t.Fatalf("CompareValues()=%d, want %d", got, tt.wantCmp)
			}
			if got := AreValuesEqual(tt.a, tt.b, tt.colType); got != tt.wantEq {
				t.Fatalf("AreValuesEqual()=%v, want %v", got, tt.wantEq)
			}
		})
	}

	if AreValuesEqual(nil, "", "text") {
		t.Fatal("NULL and empty string must remain distinct")
	}
}

func TestStreamCompareDataReportsZeroPrimaryKey(t *testing.T) {
	cols := []string{"id", "value"}
	types := map[string]string{"id": "bigint", "value": "text"}
	src := &mockDB{
		rows:     []conn.Record{{"id": int64(0), "value": "zero"}, {"id": int64(1), "value": "one"}},
		cols:     cols,
		pk:       []string{"id"},
		colTypes: types,
	}
	tgt := &mockDB{
		rows:     []conn.Record{{"id": int64(1), "value": "one"}},
		cols:     cols,
		pk:       []string{"id"},
		colTypes: types,
	}
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"value"})

	diff, err := StreamCompareDataToDiff(src, tgt, rule, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.Dropped) != 1 || diff.Dropped[0]["id"] != int64(0) {
		t.Fatalf("expected dropped primary key 0, got %#v", diff.Dropped)
	}
}

type verifiedHashMockDB struct {
	*mockDB
	cfg             config.ConnConfig
	stats           chunk.ChunkStats
	statsErr        error
	ranges          []chunk.ChunkRange
	hash            string
	batchCalls      int
	hashRanges      []chunk.ChunkRange
	statsCalls      int
	rangeCalls      int
	statsRangeCalls int
}

func (m *verifiedHashMockDB) GetConfig() *config.ConnConfig { return &m.cfg }

func (m *verifiedHashMockDB) GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	m.batchCalls++
	return m.mockDB.GetTableDataBatch(table, cols, pk, lastPK, limit)
}

func (m *verifiedHashMockDB) GetChunkStats(table, pk string) (chunk.ChunkStats, error) {
	m.statsCalls++
	return m.stats, m.statsErr
}

func (m *verifiedHashMockDB) GetChunkRanges(table, pk string, chunkSize int) ([]chunk.ChunkRange, error) {
	m.rangeCalls++
	return m.ranges, nil
}

func (m *verifiedHashMockDB) GetChunkRangesWithStats(table, pk string, chunkSize int, stats chunk.ChunkStats) ([]chunk.ChunkRange, error) {
	m.statsRangeCalls++
	if !reflect.DeepEqual(stats, m.stats) {
		return nil, errors.New("caller did not reuse verified stats")
	}
	return m.ranges, nil
}

func (m *verifiedHashMockDB) GetChunkHash(table string, cols []string, pk string, minPK, maxPK any, isLast bool) (string, error) {
	m.hashRanges = append(m.hashRanges, chunk.ChunkRange{MinPK: minPK, MaxPK: maxPK, IsLast: isLast})
	return m.hash, nil
}

func newVerifiedHashMock(rows []conn.Record, stats chunk.ChunkStats) *verifiedHashMockDB {
	return &verifiedHashMockDB{
		mockDB: &mockDB{
			rows:     rows,
			cols:     []string{"id", "value"},
			pk:       []string{"id"},
			colTypes: map[string]string{"id": "bigint", "value": "text"},
		},
		cfg:    config.ConnConfig{Type: consts.DBTypePostgres},
		stats:  stats,
		ranges: []chunk.ChunkRange{{ChunkIndex: 0, MinPK: nil, MaxPK: int64(1)}, {ChunkIndex: 1, MinPK: int64(1), MaxPK: nil, IsLast: true}},
		hash:   "same-probabilistic-fingerprint",
	}
}

func TestChunkHashSkipRequiresStatsAndUsesUnboundedFirstRange(t *testing.T) {
	stats := chunk.ChunkStats{Count: 2, MinPK: int64(0), MaxPK: int64(1)}
	src := newVerifiedHashMock([]conn.Record{{"id": int64(0)}, {"id": int64(1)}}, stats)
	tgt := newVerifiedHashMock([]conn.Record{{"id": int64(0)}, {"id": int64(1)}}, stats)
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"value"})

	err := StreamCompareDataWithChunkFilter(src, tgt, rule, 1, 1, func(DiffType, conn.Record, conn.Record, []string) {
		t.Fatal("matching verified chunks should skip row-level comparison")
	})
	if err != nil {
		t.Fatal(err)
	}
	if src.batchCalls != 0 || tgt.batchCalls != 0 {
		t.Fatalf("row scan occurred: source=%d target=%d", src.batchCalls, tgt.batchCalls)
	}
	if src.statsCalls != 1 || tgt.statsCalls != 1 || tgt.statsRangeCalls != 1 || tgt.rangeCalls != 0 {
		t.Fatalf("unexpected stats/range query counts: src stats=%d target stats=%d stats-aware ranges=%d legacy ranges=%d", src.statsCalls, tgt.statsCalls, tgt.statsRangeCalls, tgt.rangeCalls)
	}
	if len(src.hashRanges) == 0 || src.hashRanges[0].MinPK != nil || src.hashRanges[0].MaxPK != int64(1) {
		t.Fatalf("first hash range was not unbounded below: %#v", src.hashRanges)
	}
}

func TestChunkHashStatsMismatchFallsBackAndReportsLowSourceKey(t *testing.T) {
	src := newVerifiedHashMock(
		[]conn.Record{{"id": int64(0), "value": "zero"}, {"id": int64(1), "value": "one"}},
		chunk.ChunkStats{Count: 2, MinPK: int64(0), MaxPK: int64(1)},
	)
	tgt := newVerifiedHashMock(
		[]conn.Record{{"id": int64(1), "value": "one"}},
		chunk.ChunkStats{Count: 1, MinPK: int64(1), MaxPK: int64(1)},
	)
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"value"})
	var got []int64
	err := StreamCompareDataWithChunkFilter(src, tgt, rule, 2, 1, func(kind DiffType, srcRow, _ conn.Record, _ []string) {
		if kind == DiffTypeDrop {
			got = append(got, srcRow["id"].(int64))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int64{0}) {
		t.Fatalf("expected source key 0 to be reported, got %v", got)
	}
	if len(src.hashRanges) != 0 || len(tgt.hashRanges) != 0 {
		t.Fatal("hashes must not be consulted when count/min/max differ")
	}
}

func TestChunkStatsErrorsPropagate(t *testing.T) {
	wantErr := errors.New("interrupted stats cursor")
	src := newVerifiedHashMock(nil, chunk.ChunkStats{})
	src.statsErr = wantErr
	tgt := newVerifiedHashMock(nil, chunk.ChunkStats{})
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"value"})
	err := StreamCompareDataWithChunkFilter(src, tgt, rule, 1, 1, func(DiffType, conn.Record, conn.Record, []string) {})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected stats error to propagate, got %v", err)
	}
}

func TestInvalidBatchAndChunkSizesFailBeforeDatabaseWork(t *testing.T) {
	db := newVerifiedHashMock(nil, chunk.ChunkStats{})
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"value"})
	handle := func(DiffType, conn.Record, conn.Record, []string) {}

	if err := StreamCompareDataDetailed(db, db, rule, 0, handle); err == nil {
		t.Fatal("expected invalid batch size error")
	}
	if db.batchCalls != 0 {
		t.Fatal("invalid batch size reached database work")
	}
	if err := StreamCompareDataWithChunkFilter(db, db, rule, 1, 0, handle); err == nil {
		t.Fatal("expected invalid chunk size error")
	}
	if db.batchCalls != 0 || len(db.hashRanges) != 0 {
		t.Fatal("invalid chunk size reached database work")
	}
}

func TestErrorAwareStreamingHandlerStopsBeforeNextBatch(t *testing.T) {
	rows := []conn.Record{{"id": int64(1), "value": "one"}, {"id": int64(2), "value": "two"}}
	src := newVerifiedHashMock(rows, chunk.ChunkStats{})
	tgt := newVerifiedHashMock(nil, chunk.ChunkStats{})
	table, err := tgt.ExtractTable("t")
	if err != nil {
		t.Fatal(err)
	}
	rule := CreateCompareRule(table, []string{"value"})
	wantErr := errors.New("output write failed")
	err = StreamCompareDataDetailedWithTable(src, tgt, rule, table, 1, func(DiffType, conn.Record, conn.Record, []string) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if src.batchCalls != 1 || tgt.batchCalls != 1 {
		t.Fatalf("comparison read past callback failure: source batches=%d target batches=%d", src.batchCalls, tgt.batchCalls)
	}
}
