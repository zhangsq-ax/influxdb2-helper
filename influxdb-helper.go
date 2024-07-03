package influxdb2_helper

import (
	"context"

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

func (ih *InfluxdbHelper) Write(ctx context.Context, point ...*write.Point) error {
	writeAPI := ih.getWriteAPI()
	return writeAPI.WritePoint(ctx, point...)
}

func (ih *InfluxdbHelper) WriteByGenerator(ctx context.Context, generator func(data []byte, measurement string) (*write.Point, error), data []byte, measurement string) error {
	point, err := generator(data, measurement)
	if err != nil {
		return err
	}
	return ih.Write(ctx, point)
}

func (ih *InfluxdbHelper) Query(ctx context.Context, query string) (*api.QueryTableResult, error) {
	queryAPI := ih.getQueryAPI()
	return queryAPI.Query(ctx, query)
}

func (ih *InfluxdbHelper) QueryByOptions(ctx context.Context, opts *QueryOptions) (*api.QueryTableResult, error) {
	query, err := opts.String()
	if err != nil {
		return nil, err
	}
	return ih.Query(ctx, query)
}
