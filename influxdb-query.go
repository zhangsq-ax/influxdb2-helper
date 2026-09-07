package influxdb2_helper

import (
	"fmt"
	"sort"
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
	if qo.TimeRange == nil {
		return fmt.Errorf("timeRange is required")
	}
	if !isMillisecondTimestamp((*qo.TimeRange)[0]) || !isMillisecondTimestamp((*qo.TimeRange)[1]) {
		return fmt.Errorf("timeRange must be millisecond timestamp")
	}
	if qo.Fields == nil || len(qo.Fields) == 0 {
		return fmt.Errorf("fields is required")
	}
	return nil
}

func (qo *QueryOptions) String() string {
	query := []string{
		fluxFromClause(qo.BucketName),
		fluxRangeClause(qo.TimeRange),
		fluxPredicateFilter(qo.Measurement, qo.Where, qo.Fields),
	}

	if shouldDropStartStop(qo.Columns) {
		query = append(query, `drop(columns: ["_start", "_stop"])`)
	}

	query = append(query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)
	query = append(query, `group(columns: [])`)

	if qo.DescSort {
		query = append(query, `sort(columns: ["_time"], desc: true)`)
	}

	if len(qo.Columns) > 0 {
		query = append(query, fmt.Sprintf(`keep(columns: ["%s"])`, strings.Join(qo.Columns, `","`)))
	}

	if qo.Limit > 0 {
		query = append(query, fmt.Sprintf(`limit(n: %d, offset: %d)`, qo.Limit, qo.Offset))
	}

	return strings.Join(query, "\n|> ")
}

func (qo *QueryOptions) CountString(field string) string {
	query := []string{
		fluxFromClause(qo.BucketName),
		fluxRangeClause(qo.TimeRange),
		fluxPredicateFilter(qo.Measurement, qo.Where, []string{field}),
		`count()`,
		`group(columns: [])`,
		`sum()`,
	}

	return strings.Join(query, "\n|> ")
}

func fluxFromClause(bucketName string) string {
	return fmt.Sprintf(`from(bucket: "%s")`, bucketName)
}

func fluxRangeClause(timeRange *[2]int64) string {
	startNs := timeRange[0] * 1_000_000
	stopNs := (timeRange[1] + 1) * 1_000_000
	return fmt.Sprintf(`range(start: time(v: %d), stop: time(v: %d))`, startNs, stopNs)
}

func fluxPredicateFilter(measurement string, where map[string]string, fields []string) string {
	preds := []string{
		fmt.Sprintf(`r["_measurement"] == "%s"`, measurement),
	}

	if len(where) > 0 {
		keys := make([]string, 0, len(where))
		for key := range where {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			preds = append(preds, fmt.Sprintf(`r["%s"] == "%s"`, key, where[key]))
		}
	}

	if len(fields) == 1 {
		preds = append(preds, fmt.Sprintf(`r._field == "%s"`, fields[0]))
	} else if len(fields) > 1 {
		ors := make([]string, 0, len(fields))
		for _, field := range fields {
			ors = append(ors, fmt.Sprintf(`r._field == "%s"`, field))
		}
		preds = append(preds, fmt.Sprintf(`(%s)`, strings.Join(ors, " or ")))
	}

	return fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(preds, " and "))
}

func shouldDropStartStop(columns []string) bool {
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		if column == "_start" || column == "_stop" {
			return false
		}
	}
	return true
}

func isMillisecondTimestamp(ts int64) bool {
	// 检查时间戳是否为毫秒级（通过值范围）
	// 秒级时间戳范围大致为 0 到当前时间的值（< 2^31）
	// 毫秒级时间戳范围为 10^12 到 10^13
	if ts > 1000000000000 && ts < 10000000000000 {
		return true
	}
	return false
}
