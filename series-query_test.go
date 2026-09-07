package influxdb2_helper

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesQueryOptionsString_LimitBeforePivotNoUngroup(t *testing.T) {
	opts := &SeriesQueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Where: map[string]string{
			"deviceId": "71922044000721a",
		},
		Fields:   []string{"x", "y"},
		Columns:  []string{"_time", "deviceId", "x", "y"},
		Limit:    100,
		Offset:   0,
		DescSort: true,
	}

	query := opts.String()

	assert.NotContains(t, query, `group(columns: [])`)
	assert.Contains(t, query, `range(start: time(v: 1721059200000000000), stop: time(v: 1721106016001000000))`)
	assert.Contains(t, query, `filter(fn: (r) => r["_measurement"] == "iot_state" and r["deviceId"] == "71922044000721a" and (r._field == "x" or r._field == "y"))`)
	assert.Contains(t, query, `drop(columns: ["_start", "_stop"])`)

	limitIdx := strings.Index(query, `limit(n: 100, offset: 0)`)
	pivotIdx := strings.Index(query, `pivot(rowKey: ["_time"]`)
	require.GreaterOrEqual(t, limitIdx, 0)
	require.GreaterOrEqual(t, pivotIdx, 0)
	assert.Less(t, limitIdx, pivotIdx)

	assert.Equal(t, `from(bucket: "default")
|> range(start: time(v: 1721059200000000000), stop: time(v: 1721106016001000000))
|> filter(fn: (r) => r["_measurement"] == "iot_state" and r["deviceId"] == "71922044000721a" and (r._field == "x" or r._field == "y"))
|> drop(columns: ["_start", "_stop"])
|> sort(columns: ["_time"], desc: true)
|> limit(n: 100, offset: 0)
|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
|> keep(columns: ["_time","deviceId","x","y"])`, query)
}

func TestSeriesQueryOptionsString_CustomPivotKeys(t *testing.T) {
	opts := &SeriesQueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Fields:      []string{"x"},
		Limit:       10,
		PivotKeys:   []string{"_time", "deviceId"},
	}

	query := opts.String()
	assert.Contains(t, query, `pivot(rowKey: ["_time","deviceId"], columnKey: ["_field"], valueColumn: "_value")`)
	assert.NotContains(t, query, `group(columns: [])`)
}

func TestSeriesQueryOptionsFluxString_ProbeLimit(t *testing.T) {
	opts := &SeriesQueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Fields:      []string{"x"},
		Limit:       100,
	}

	assert.Contains(t, opts.String(), `limit(n: 100, offset: 0)`)
	assert.Contains(t, opts.fluxString(opts.Limit+1), `limit(n: 101, offset: 0)`)
}

func TestSeriesQueryOptionsValidate(t *testing.T) {
	opts := &SeriesQueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Fields:      []string{"x"},
		Limit:       10,
	}
	assert.NoError(t, opts.Validate())

	opts.Limit = 0
	assert.EqualError(t, opts.Validate(), "limit must be greater than 0")

	opts.Limit = 10
	opts.BucketName = ""
	assert.EqualError(t, opts.Validate(), "bucketName is required")

	opts.BucketName = "default"
	opts.Measurement = ""
	assert.EqualError(t, opts.Validate(), "measurement is required")

	opts.Measurement = "iot_state"
	opts.Fields = nil
	assert.EqualError(t, opts.Validate(), "fields is required")
}

func TestBuildSeriesPage_SingleTableHasMore(t *testing.T) {
	tables := [][]map[string]interface{}{
		{
			{"_time": 1, "x": 1.0},
			{"_time": 2, "x": 2.0},
			{"_time": 3, "x": 3.0},
		},
	}

	page := buildSeriesPage(tables, 2)
	assert.True(t, page.HasMore)
	require.Len(t, page.Records, 2)
	assert.Equal(t, 1, page.Records[0]["_time"])
	assert.Equal(t, 2, page.Records[1]["_time"])
}

func TestBuildSeriesPage_MultiTableHasMore(t *testing.T) {
	tables := [][]map[string]interface{}{
		{
			{"deviceId": "a", "_time": 1},
			{"deviceId": "a", "_time": 2},
		},
		{
			{"deviceId": "b", "_time": 1},
			{"deviceId": "b", "_time": 2},
			{"deviceId": "b", "_time": 3},
		},
	}

	page := buildSeriesPage(tables, 2)
	assert.True(t, page.HasMore)
	require.Len(t, page.Records, 4)
	assert.Equal(t, "a", page.Records[0]["deviceId"])
	assert.Equal(t, "a", page.Records[1]["deviceId"])
	assert.Equal(t, "b", page.Records[2]["deviceId"])
	assert.Equal(t, "b", page.Records[3]["deviceId"])
	assert.Equal(t, 2, page.Records[3]["_time"])
}

func TestBuildSeriesPage_NoHasMore(t *testing.T) {
	tables := [][]map[string]interface{}{
		{
			{"_time": 1},
			{"_time": 2},
		},
	}

	page := buildSeriesPage(tables, 10)
	assert.False(t, page.HasMore)
	require.Len(t, page.Records, 2)
}
