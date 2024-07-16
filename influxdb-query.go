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
	Fields      []string
	Limit       int64
	Offset      int64
}

func (qo *QueryOptions) String() (string, error) {
	if qo.TimeRange == nil {
		return "", fmt.Errorf("the TimeRange is required")
	}
	startTime := qo.TimeRange[0] / 1000
	endTime := qo.TimeRange[1]/1000 + 1

	query := []string{}

	// from clause
	query = append(query, fmt.Sprintf(`from(bucket: "%s")`, qo.BucketName))

	// time range clause
	query = append(query, fmt.Sprintf(`range(start: %d, stop: %d)`, startTime, endTime))

	// measurement clause
	if qo.Measurement == "" {
		return "", fmt.Errorf("the Measurement is required")
	}
	query = append(query, fmt.Sprintf(`filter(fn: (r) => r["_measurement"] == "%s")`, qo.Measurement))

	// where clause
	where := []string{}
	if qo.Where != nil && len(qo.Where) > 0 {
		for key, val := range qo.Where {
			where = append(where, fmt.Sprintf(`r["%s"] == "%s"`, key, val))
		}
		query = append(query, fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(where, " and ")))
	}

	// select clause
	if qo.Fields != nil && len(qo.Fields) > 0 {
		selectClause := []string{}
		for _, field := range qo.Fields {
			selectClause = append(selectClause, fmt.Sprintf(`r["_field"] == "%s"`, field))
		}
		query = append(query, fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(selectClause, " or ")))
	}

	// pivot clause
	query = append(query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)

	// pagination
	if qo.Limit > 0 {
		query = append(query, fmt.Sprintf(`limit(n: %d, offset: %d)`, qo.Limit, qo.Offset))
	}

	return strings.Join(query, "\n|> "), nil
}
