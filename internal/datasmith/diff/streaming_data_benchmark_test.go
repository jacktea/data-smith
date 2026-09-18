package diff

import (
	"fmt"
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

type benchmarkCountingWriter int64

func (w *benchmarkCountingWriter) Write(p []byte) (int, error) {
	*w += benchmarkCountingWriter(len(p))
	return len(p), nil
}

func BenchmarkDataDiffPipelines(b *testing.B) {
	for _, scenario := range []struct {
		name      string
		totalRows int
		diffRows  int
	}{
		{name: "low_difference", totalRows: 10000, diffRows: 100},
		{name: "high_difference", totalRows: 100000, diffRows: 100000},
	} {
		rows := make([]conn.Record, scenario.diffRows)
		for i := range rows {
			rows[i] = conn.Record{"id": i + 1, "name": fmt.Sprintf("value-%06d", i+1), "note": "stable"}
		}
		table := streamingTestTable("items")
		rules := []config.Rule{{Table: "items"}}
		dialect := pkgsql.NewDialect(consts.DBTypeMySQL)

		b.Run(scenario.name+"/accumulated_before", func(b *testing.B) {
			b.ReportAllocs()
			var outputBytes int64
			for i := 0; i < b.N; i++ {
				var forward, rollback benchmarkCountingWriter
				compare := func(config.Rule) (*tableDiffResult, error) {
					result := &pkgdiff.DataDiff{Added: make([]conn.Record, 0, len(rows))}
					result.Added = append(result.Added, rows...)
					return &tableDiffResult{target: table, source: table, diff: result}, nil
				}
				if _, err := generateDataDiffOutputs(&forward, &rollback, rules, dialect, false, compare); err != nil {
					b.Fatal(err)
				}
				outputBytes += int64(forward + rollback)
			}
			b.ReportMetric(float64(scenario.totalRows), "rows/op")
			b.ReportMetric(float64(scenario.diffRows), "diff-rows/op")
			b.ReportMetric(float64(scenario.diffRows), "peak-buffered-rows/op")
			b.ReportMetric(float64(outputBytes)/float64(b.N), "output-bytes/op")
		})

		b.Run(scenario.name+"/streaming_after", func(b *testing.B) {
			b.ReportAllocs()
			spoolParent := b.TempDir()
			var outputBytes int64
			prepare := func(config.Rule) (*tableModels, error) {
				return &tableModels{target: table, source: table}, nil
			}
			compare := func(_ config.Rule, _ *tableModels, handle pkgdiff.DetailedDiffErrorHandler) error {
				for _, row := range rows {
					if err := handle(pkgdiff.DiffTypeAdd, nil, row, nil); err != nil {
						return err
					}
				}
				return nil
			}
			for i := 0; i < b.N; i++ {
				var forward, rollback benchmarkCountingWriter
				if _, err := generateStreamingDataDiffOutputs(&forward, &rollback, spoolParent, rules, dialect, 1000, false, prepare, compare); err != nil {
					b.Fatal(err)
				}
				outputBytes += int64(forward + rollback)
			}
			b.ReportMetric(float64(scenario.totalRows), "rows/op")
			b.ReportMetric(float64(scenario.diffRows), "diff-rows/op")
			peakRows := scenario.diffRows * 2
			if peakRows > 2000 {
				peakRows = 2000
			}
			b.ReportMetric(float64(peakRows), "peak-buffered-rows/op")
			b.ReportMetric(float64(outputBytes)/float64(b.N), "output-bytes/op")
		})
	}
}
