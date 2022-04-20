package influxdb2_helper

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
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

func TestInfluxdbHelper_Query(t *testing.T) {
	now := time.Now()
	queryOptions := &QueryOptions{
		TimeRange:   &[2]int64{now.UnixMilli() - 5000, now.UnixMilli()},
		BucketName:  os.Getenv("BUCKET_NAME"),
		Measurement: os.Getenv("MEASUREMENT"),
		Where: map[string]string{
			"deviceId": "71922044000721a",
		},
		Fields: []string{"rawData"},
	}
	query, err := queryOptions.String()
	assert.NoError(t, err)
	t.Log(query)

	result, err := helper.Query(context.Background(), query)
	assert.NoError(t, err)
	for result.Next() {
		fmt.Println(result.Record().Values())
	}
}
