package influxdb2_helper

import (
	"fmt"
	"strings"
)

type QueryOptions struct {
	TimeRange   *[2]int64
	BucketName  string
	Measurement string
	Where       map[string]string
	Columns     []string
	Limit       int64
	Offset      int64
	DescSort    bool
}

func (qo *QueryOptions) String() string {
	startTime := qo.TimeRange[0] / 1000
	endTime := qo.TimeRange[1]/1000 + 1

	query := []string{}

	// from clause
	query = append(query, fmt.Sprintf(`from(bucket: "%s")`, qo.BucketName))

	// time range clause
	query = append(query, fmt.Sprintf(`range(start: %d, stop: %d)`, startTime, endTime))

	// measurement clause
	query = append(query, fmt.Sprintf(`filter(fn: (r) => r["_measurement"] == "%s")`, qo.Measurement))

	// where clause
	where := []string{}
	if qo.Where != nil && len(qo.Where) > 0 {
		for key, val := range qo.Where {
			where = append(where, fmt.Sprintf(`r["%s"] == "%s"`, key, val))
		}
		query = append(query, fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(where, " and ")))
	}

	// pivot clause
	query = append(query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)

	// sort clause
	if qo.DescSort {
		query = append(query, `sort(columns: ["_time"], desc: true)`)
	}

	// select clause
	if qo.Columns != nil && len(qo.Columns) > 0 {
		query = append(query, fmt.Sprintf(`keep(columns: ["%s"])`, strings.Join(qo.Columns, `","`)))
	}

	// pagination
	if qo.Limit > 0 {
		query = append(query, fmt.Sprintf(`limit(n: %d, offset: %d)`, qo.Limit, qo.Offset))
	}

	return strings.Join(query, "\n|> ")
}

func (qo *QueryOptions) CountString(column string) string {
	startTime := qo.TimeRange[0] / 1000
	endTime := qo.TimeRange[1]/1000 + 1

	query := []string{}

	// from clause
	query = append(query, fmt.Sprintf(`from(bucket: "%s")`, qo.BucketName))

	// time range clause
	query = append(query, fmt.Sprintf(`range(start: %d, stop: %d)`, startTime, endTime))

	// measurement clause
	query = append(query, fmt.Sprintf(`filter(fn: (r) => r["_measurement"] == "%s")`, qo.Measurement))

	// where clause
	where := []string{}
	if qo.Where != nil && len(qo.Where) > 0 {
		for key, val := range qo.Where {
			where = append(where, fmt.Sprintf(`r["%s"] == "%s"`, key, val))
		}
		query = append(query, fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(where, " and ")))
	}

	// only select specified column
	query = append(query, fmt.Sprintf(`filter(fn: (r) => r["_field"] == "%s")`, column))

	// pivot clause
	query = append(query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)

	query = append(query, fmt.Sprintf(`keep(columns: ["%s"])`, column))

	query = append(query, fmt.Sprintf(`count(column: "%s")`, column))

	return strings.Join(query, "\n|> ")
}
