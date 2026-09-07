package influxdb2_helper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleTimeRange() *[2]int64 {
	tr := [2]int64{1721059200000, 1721106016000}
	return &tr
}

func TestQueryOptionsString_MergedFilterAndMillisecondRange(t *testing.T) {
	opts := &QueryOptions{
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

	assert.Contains(t, query, `from(bucket: "default")`)
	assert.Contains(t, query, `range(start: time(v: 1721059200000000000), stop: time(v: 1721106016001000000))`)
	assert.Contains(t, query, `filter(fn: (r) => r["_measurement"] == "iot_state" and r["deviceId"] == "71922044000721a" and (r._field == "x" or r._field == "y"))`)
	assert.Contains(t, query, `drop(columns: ["_start", "_stop"])`)
	assert.Contains(t, query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)
	assert.Contains(t, query, `group(columns: [])`)
	assert.Contains(t, query, `sort(columns: ["_time"], desc: true)`)
	assert.Contains(t, query, `keep(columns: ["_time","deviceId","x","y"])`)
	assert.Contains(t, query, `limit(n: 100, offset: 0)`)

	assert.Equal(t, `from(bucket: "default")
|> range(start: time(v: 1721059200000000000), stop: time(v: 1721106016001000000))
|> filter(fn: (r) => r["_measurement"] == "iot_state" and r["deviceId"] == "71922044000721a" and (r._field == "x" or r._field == "y"))
|> drop(columns: ["_start", "_stop"])
|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
|> group(columns: [])
|> sort(columns: ["_time"], desc: true)
|> keep(columns: ["_time","deviceId","x","y"])
|> limit(n: 100, offset: 0)`, query)
}

func TestQueryOptionsString_NoDropWhenColumnsEmpty(t *testing.T) {
	opts := &QueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Fields:      []string{"x"},
	}

	query := opts.String()

	assert.NotContains(t, query, `drop(columns: ["_start", "_stop"])`)
	assert.Contains(t, query, `filter(fn: (r) => r["_measurement"] == "iot_state" and r._field == "x")`)
	assert.Contains(t, query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)
	assert.Contains(t, query, `group(columns: [])`)
}

func TestQueryOptionsString_NoDropWhenColumnsKeepStartStop(t *testing.T) {
	opts := &QueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Fields:      []string{"x"},
		Columns:     []string{"_time", "_start", "x"},
	}

	query := opts.String()
	assert.NotContains(t, query, `drop(columns: ["_start", "_stop"])`)
	assert.Contains(t, query, `keep(columns: ["_time","_start","x"])`)
}

func TestQueryOptionsString_WhereKeysAreStable(t *testing.T) {
	opts := &QueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Where: map[string]string{
			"zone":     "a",
			"deviceId": "dev-1",
		},
		Fields: []string{"x", "y", "yaw"},
	}

	query := opts.String()
	require.Contains(t, query, `r["deviceId"] == "dev-1" and r["zone"] == "a"`)
	assert.Contains(t, query, `(r._field == "x" or r._field == "y" or r._field == "yaw")`)
}

func TestQueryOptionsCountString_MergedFilterAndSum(t *testing.T) {
	opts := &QueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Where: map[string]string{
			"deviceId": "71922044000721a",
		},
		Fields: []string{"x", "y"},
	}

	query := opts.CountString("x")

	assert.Equal(t, `from(bucket: "default")
|> range(start: time(v: 1721059200000000000), stop: time(v: 1721106016001000000))
|> filter(fn: (r) => r["_measurement"] == "iot_state" and r["deviceId"] == "71922044000721a" and r._field == "x")
|> count()
|> group(columns: [])
|> sum()`, query)
}

func TestQueryOptionsValidate(t *testing.T) {
	opts := &QueryOptions{
		TimeRange:   sampleTimeRange(),
		BucketName:  "default",
		Measurement: "iot_state",
		Fields:      []string{"x"},
	}
	assert.NoError(t, opts.Validate())

	opts.Fields = nil
	assert.EqualError(t, opts.Validate(), "fields is required")

	opts.Fields = []string{"x"}
	opts.TimeRange = nil
	assert.EqualError(t, opts.Validate(), "timeRange is required")
}
