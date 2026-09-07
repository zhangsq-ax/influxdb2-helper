package influxdb2_helper

import (
	"fmt"
	"strings"
)

// SeriesQueryOptions is a high-performance query path where Limit/Offset apply
// per series (matching Flux table boundaries). Unlike QueryOptions, it does not
// ungroup with group(columns: []) and therefore does not provide a global Limit
// across all tags. Prefer this for single-device (or other tagged) list pages.
type SeriesQueryOptions struct {
	TimeRange   *[2]int64
	BucketName  string
	Measurement string
	Where       map[string]string
	Fields      []string // required
	Columns     []string
	Limit       int64 // required, > 0; per-series
	Offset      int64 // per-series; prefer narrowing TimeRange for deep pages
	DescSort    bool
	PivotKeys   []string // empty defaults to ["_time"]
}

// SeriesPageResult is a buffered page from QuerySeriesPage.
// Records are truncated to at most Limit rows per series.
// HasMore is true if at least one series had more than Limit rows.
type SeriesPageResult struct {
	Records []map[string]interface{}
	HasMore bool
}

func (o *SeriesQueryOptions) Validate() error {
	if o.TimeRange == nil {
		return fmt.Errorf("timeRange is required")
	}
	if !isMillisecondTimestamp((*o.TimeRange)[0]) || !isMillisecondTimestamp((*o.TimeRange)[1]) {
		return fmt.Errorf("timeRange must be millisecond timestamp")
	}
	if o.BucketName == "" {
		return fmt.Errorf("bucketName is required")
	}
	if o.Measurement == "" {
		return fmt.Errorf("measurement is required")
	}
	if len(o.Fields) == 0 {
		return fmt.Errorf("fields is required")
	}
	if o.Limit <= 0 {
		return fmt.Errorf("limit must be greater than 0")
	}
	return nil
}

// String builds Flux using Limit (not Limit+1), for debugging and tests.
func (o *SeriesQueryOptions) String() string {
	return o.fluxString(o.Limit)
}

func (o *SeriesQueryOptions) fluxString(limit int64) string {
	query := []string{
		fluxFromClause(o.BucketName),
		fluxRangeClause(o.TimeRange),
		fluxPredicateFilter(o.Measurement, o.Where, o.Fields),
	}

	if shouldDropStartStop(o.Columns) {
		query = append(query, `drop(columns: ["_start", "_stop"])`)
	}

	if o.DescSort {
		query = append(query, `sort(columns: ["_time"], desc: true)`)
	}

	query = append(query, fmt.Sprintf(`limit(n: %d, offset: %d)`, limit, o.Offset))

	pivotKeys := o.PivotKeys
	if len(pivotKeys) == 0 {
		pivotKeys = []string{"_time"}
	}
	query = append(query, fmt.Sprintf(
		`pivot(rowKey: ["%s"], columnKey: ["_field"], valueColumn: "_value")`,
		strings.Join(pivotKeys, `","`),
	))

	if len(o.Columns) > 0 {
		query = append(query, fmt.Sprintf(`keep(columns: ["%s"])`, strings.Join(o.Columns, `","`)))
	}

	return strings.Join(query, "\n|> ")
}

// buildSeriesPage truncates each table to limit rows and sets HasMore if any
// table had more than limit rows. tables are in Flux result order.
func buildSeriesPage(tables [][]map[string]interface{}, limit int64) *SeriesPageResult {
	result := &SeriesPageResult{
		Records: make([]map[string]interface{}, 0),
	}
	if limit <= 0 {
		for _, table := range tables {
			result.Records = append(result.Records, table...)
		}
		return result
	}

	for _, table := range tables {
		n := int64(len(table))
		if n > limit {
			result.HasMore = true
			result.Records = append(result.Records, table[:limit]...)
		} else {
			result.Records = append(result.Records, table...)
		}
	}
	return result
}

func copyRecordValues(values map[string]interface{}) map[string]interface{} {
	copied := make(map[string]interface{}, len(values))
	for k, v := range values {
		copied[k] = v
	}
	return copied
}
