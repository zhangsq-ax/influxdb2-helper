package influxdb2_helper

import (
	"fmt"
	"strings"
)

type QueryOptions struct {
	TimeRange   *[2]int64 // 时间范围，[开始时间,结束时间]，单位为毫秒
	BucketName  string
	Measurement string
	Where       map[string]string // 过滤条件
	Fields      []string          // 指定要获取的 Field 字段，必须指定，否则有性能问题
	Columns     []string          // 查询结果要返回的字段列表
	Limit       int64             // 限制返回的记录数
	Offset      int64             // 查询结果偏移
	DescSort    bool              // 是否按时间倒序
}

func (qo *QueryOptions) Validate() error {
	if qo.Fields == nil || len(qo.Fields) == 0 {
		return fmt.Errorf("fields is required")
	}
	return nil
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

	// field clause
	fields := []string{}
	for _, field := range qo.Fields {
		fields = append(fields, fmt.Sprintf(`r._field == "%s"`, field))
	}
	query = append(query, fmt.Sprintf(`filter(fn: (r) => %s`, strings.Join(fields, " or ")))

	// pivot clause
	query = append(query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)

	// force tags not to be grouped
	query = append(query, `group(columns: [])`)

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

func (qo *QueryOptions) CountString(field string) string {
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

	// only select specified field
	query = append(query, fmt.Sprintf(`filter(fn: (r) => r._field == "%s")`, field))

	// pivot clause
	query = append(query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)

	query = append(query, fmt.Sprintf(`keep(columns: ["%s"])`, field))

	query = append(query, fmt.Sprintf(`count(column: "%s")`, field))

	return strings.Join(query, "\n|> ")
}
