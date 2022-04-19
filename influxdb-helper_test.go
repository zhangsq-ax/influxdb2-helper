package influxdb2_helper

import (
	"os"
	"testing"
)

var helper *InfluxdbHelper

func TestNewInfluxdbHelper(t *testing.T) {
	helper = NewInfluxdbHelper(&InfluxdbHelperOptions{
		ServerUrl:  os.Getenv("SERVER_URL"),
		Token:      os.Getenv("TOKEN"),
		OrgName:    os.Getenv("ORG_NAME"),
		BucketName: os.Getenv("BUCKET_NAME"),
	})
}

/* func TestInfluxdbHelper_Query(t *testing.T) {
	err := helper.Query(context.Background(), `from(bucket: "default")
	|> range(start: -5s)
	|> filter(fn: (r) => r["_measurement"] == "robot_state" and r["deviceId"] == "718220110000202")
	|> filter(fn: (r) => r["_field"] == "x" or r["_field"] == "y" or r["_field"] == "yaw")
	|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)
	assert.NoError(t, err)
} */

/*func TestQueryOptions(t *testing.T) {
/* opts := &QueryOptions{
	TimeRange:   [2]int64{1644924507758, 1644925793289},
	Measurement: "robot_state",
	Where: map[string]string{
		"deviceId": "718220110000202",
	},
	Fields: []string{"x", "y", "yaw"},
} */

/*timeRange := [2]int64{1644924507758, 1644925793289}

	opts := &QueryOptions{
		TimeRange:   &timeRange,
		Measurement: "robot_state",
	}
	t.Log(opts.ToString("default"))

	opts.Where = map[string]string{
		"deviceId": "718220110000202",
	}
	t.Log(opts.ToString("default"))

	opts.Fields = []string{"x", "y", "yaw"}
	t.Log(opts.ToString("default"))
}

func TestInfluxdbHelper_Query(t *testing.T) {
	now := time.Now()
	queryOptions := &QueryOptions{
		TimeRange:   &[2]int64{now.UnixMilli() - 5000, now.UnixMilli()},
		Measurement: "robot_state",
	}
	query, err := queryOptions.ToString("default")
	assert.NoError(t, err)
	t.Log(query)

	result, err := helper.Query(context.Background(), query)
	assert.NoError(t, err)
	for result.Next() {
		fmt.Println(result.Record().Values())
	}
}*/

/*type QueryOptions struct {
	TimeRange   *[2]int64
	Measurement string
	Where       map[string]string
	Fields      []string
}

func (qo *QueryOptions) ToString(bucketName string) (string, error) {
	if qo.TimeRange == nil {
		return "", fmt.Errorf("the TimeRange is required")
	}
	startTime := qo.TimeRange[0] / 1000
	endTime := qo.TimeRange[1]/1000 + 1

	query := []string{}

	// from clause
	query = append(query, fmt.Sprintf(`from(bucket: "%s")`, bucketName))

	// range clause
	query = append(query, fmt.Sprintf(`range(start: %d, stop: %d)`, startTime, endTime))

	if qo.Measurement == "" {
		return "", fmt.Errorf("the Measurement is required")
	}
	where := []string{}
	if qo.Where != nil {
		for key, val := range qo.Where {
			where = append(where, fmt.Sprintf(` and r["%s"] == "%s"`, key, val))
		}
	}
	// where clause
	query = append(query, fmt.Sprintf(`filter(fn: (r) => r["_measurement"] == "%s"%s)`, qo.Measurement, strings.Join(where, "")))

	if qo.Fields != nil && len(qo.Fields) > 0 {
		selectClause := []string{}
		for _, field := range qo.Fields {
			selectClause = append(selectClause, fmt.Sprintf(`r["_field"] == "%s"`, field))
		}
		// select clause
		query = append(query, fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(selectClause, " or ")))
	}

	// pivot clause
	query = append(query, `pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)

	return strings.Join(query, "\n|> "), nil
}*/
