package influxdb2_helper

import (
	"context"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type InfluxdbHelperOptions struct {
	ServerUrl  string
	Token      string
	OrgName    string
	BucketName string
}

type InfluxdbHelper struct {
	opts     *InfluxdbHelperOptions
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	queryAPI api.QueryAPI
}

func NewInfluxdbHelper(opts *InfluxdbHelperOptions) *InfluxdbHelper {
	return &InfluxdbHelper{
		opts:   opts,
		client: influxdb2.NewClient(opts.ServerUrl, opts.Token),
	}
}

func (ih *InfluxdbHelper) getWriteAPI() api.WriteAPIBlocking {
	if ih.writeAPI == nil {
		ih.writeAPI = ih.client.WriteAPIBlocking(ih.opts.OrgName, ih.opts.BucketName)
	}
	return ih.writeAPI
}

func (ih *InfluxdbHelper) getQueryAPI() api.QueryAPI {
	if ih.queryAPI == nil {
		ih.queryAPI = ih.client.QueryAPI(ih.opts.OrgName)
	}

	return ih.queryAPI
}

func (ih *InfluxdbHelper) BucketName() string {
	return ih.opts.BucketName
}

func (ih *InfluxdbHelper) Write(ctx context.Context, point ...*write.Point) error {
	writeAPI := ih.getWriteAPI()
	return writeAPI.WritePoint(ctx, point...)
}

func (ih *InfluxdbHelper) WriteByGenerator(ctx context.Context, measurement string, data any, generator func(data any, measurement string) (*write.Point, error)) (*write.Point, error) {
	point, err := generator(data, measurement)
	if err != nil {
		return nil, err
	}
	return point, ih.Write(ctx, point)
}

func (ih *InfluxdbHelper) query(ctx context.Context, query string) (*api.QueryTableResult, error) {
	queryAPI := ih.getQueryAPI()
	return queryAPI.Query(ctx, query)
}

func (ih *InfluxdbHelper) QueryByOptions(ctx context.Context, opts *QueryOptions) (*api.QueryTableResult, error) {
	if opts == nil {
		return nil, fmt.Errorf("opts is required")
	}
	err := opts.Validate()
	if err != nil {
		return nil, err
	}
	return ih.query(ctx, opts.String())
}

// Count returns the count of the column in the query
// column must be a field in the measurement, not a tag
func (ih *InfluxdbHelper) Count(ctx context.Context, opts *QueryOptions, field string) (int64, error) {
	if opts == nil {
		return 0, fmt.Errorf("opts is required")
	}
	query := opts.CountString(field)
	result, err := ih.query(ctx, query)
	if err != nil {
		return 0, err
	}
	count := int64(0)
	for result.Next() {
		count += result.Record().ValueByKey(field).(int64)
	}
	return count, nil
}

func (ih *InfluxdbHelper) NewQueryOptions(measurement string, where map[string]string, columns []string, startTime int64, endTime int64, limit int64, offset int64) *QueryOptions {
	timeRange := [2]int64{startTime, endTime}
	return &QueryOptions{
		TimeRange:   &timeRange,
		BucketName:  ih.opts.BucketName,
		Measurement: measurement,
		Where:       where,
		Columns:     columns,
		Limit:       limit,
		Offset:      offset,
	}
}
