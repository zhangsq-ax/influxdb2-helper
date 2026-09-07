# influxdb2-helper

Designed to simplify operations of InfluxDB v2

## Usage

### Import Module

```go
package main

import idbHelper "github.com/zhangsq-ax/influxdb2-helper"

...
```

### Create Helper Instance

```go
...
helper := idbHelper.NewInfluxdbHelper(&idbHelper.InfluxdbHelperOptions{
    ServerUrl: os.Getenv("SERVER_URL"),
    Token: os.Getenv("TOKEN"),
    OrgName: os.Getenv("ORG_NAME"),
    BucketName: os.Getenv("BUCKET_NAME")
})
...
```

### Query

#### Build Query Options

```go
...
queryOpts := &idbHelper.QueryOptions{
    TimeRange: &[2]int64{1721059200000, 1721106016000},
    BucketName: "default",
    Measurement: "iot_state",
    Where: map[string]string{
        "deviceId": "71922044000721a",
    },
    Fields: []string{"x", "y", "yaw"},
    Columns: []string{"_time", "deviceId", "x", "y", "yaw"},
    Limit: 100,
    Offset: 0,
    DescSort: true,
}
...
```

or 

```go
...
queryOpts := helper.NewQueryOptions("iot_state", map[string]string{}, []string{"_time", "deviceId", "..."}, 1721059200000, 1721106016000, 100, 0)
queryOpts.Fields = []string{"x", "y", "yaw"}
...
```

> **TimeRange** - Required. The time range for querying data, UTC timestamp in milliseconds (inclusive).
>
> **BucketName** - Required. The name of bucket to query.
>
> **Measurement** - Required. The name of measurement to query.
>
> **Where** - Optional. Tag-based query and filter conditions. Currently only the "and" relationship is supported between multiple conditions. By default, no conditional filtering is performed.
>
> **Fields** - Required for `QueryByOptions`. Field names to fetch. Must be set to avoid scanning all fields.
>
> **Columns** - Optional. Columns returned by the query result. By default, all columns are returned.
>
> **Limit** - Optional. Global limit after ungrouping all series. By default there is no limit, which may have performance issues on large ranges.
>
> **Offset** - Optional. Records to skip, used with **Limit** for paging. Prefer narrowing **TimeRange** for deep pages.
>
> **DescSort** - Optional. Whether to sort the query results in reverse order based on time.

#### Query Data

```go
...
result, err := helper.QueryByOptions(context.Background(), queryOpts)
if err != nil {
  panic(err)
}
for result.Next() {
  fmt.Println(result.Record().Values())
}
...
```

#### Count

`Count` returns how many points match the filter for a single **field** (not a tag). It scans the full time range; for list APIs prefer `QuerySeriesPage` + `HasMore`, or cache count by query conditions.

```go
...
total, err := helper.Count(context.Background(), &idbHelper.QueryOptions{
    TimeRange:   queryOpts.TimeRange,
    BucketName:  queryOpts.BucketName,
    Measurement: queryOpts.Measurement,
    Where:       queryOpts.Where,
}, "x")
if err != nil {
    panic(err)
}
fmt.Println("total:", total)
...
```

### Series page query (high performance)

Use `QuerySeriesPage` for tagged list pages (for example filtering by `deviceId`). Compared with `QueryByOptions`:

- `Limit` / `Offset` apply **per series**, not as a global limit across all tags
- Flux does **not** run `group(columns: [])`, and applies `limit` **before** `pivot`
- The response is buffered rows plus `HasMore` (probed with `Limit+1`); it does **not** return a total count

```go
...
pageOpts := &idbHelper.SeriesQueryOptions{
    TimeRange:   &[2]int64{1721059200000, 1721106016000},
    BucketName:  "default",
    Measurement: "iot_state",
    Where: map[string]string{
        "deviceId": "71922044000721a",
    },
    Fields:   []string{"x", "y", "yaw"},
    Columns:  []string{"_time", "deviceId", "x", "y", "yaw"},
    Limit:    100,
    DescSort: true,
}
page, err := helper.QuerySeriesPage(context.Background(), pageOpts)
if err != nil {
    panic(err)
}
for _, row := range page.Records {
    fmt.Println(row)
}
if page.HasMore {
    fmt.Println("more data available; narrow TimeRange using the last row _time for the next page")
}
...
```

or

```go
...
pageOpts := helper.NewSeriesQueryOptions(
    "iot_state",
    map[string]string{"deviceId": "71922044000721a"},
    []string{"x", "y", "yaw"},
    []string{"_time", "deviceId", "x", "y", "yaw"},
    1721059200000,
    1721106016000,
    100,
    0,
)
pageOpts.DescSort = true
page, err := helper.QuerySeriesPage(context.Background(), pageOpts)
...
```

> **TimeRange** / **BucketName** / **Measurement** / **Where** / **Fields** / **Columns** / **DescSort** - Same roles as `QueryOptions`. **Fields**, **BucketName**, **Measurement**, **TimeRange**, and **Limit > 0** are required.
>
> **Limit** - Required and must be `> 0`. Applies **per series**.
>
> **Offset** - Optional per-series offset. Prefer cursor-style **TimeRange** for deep paging.
>
> **PivotKeys** - Optional. `pivot` row keys; defaults to `["_time"]`. Use e.g. `["_time", "deviceId"]` when multiple tags share the same timestamp.
>
> Prefer `QuerySeriesPage` when querying one device (or another single series). Keep using `QueryByOptions` when you need a **global** top-N across all tags after ungrouping.

#### Compatible list + total (optional)

If the application API still returns `{ items, total }`, you can load the page with `QuerySeriesPage` and call `Count` once with the same filter (same `TimeRange` / `Where` / `Measurement` / field). For single-series queries (e.g. one `deviceId`) this usually matches the old response shape.

```go
...
page, err := helper.QuerySeriesPage(context.Background(), pageOpts)
if err != nil {
    panic(err)
}
total, err := helper.Count(context.Background(), &idbHelper.QueryOptions{
    TimeRange:   pageOpts.TimeRange,
    BucketName:  pageOpts.BucketName,
    Measurement: pageOpts.Measurement,
    Where:       pageOpts.Where,
}, "x")
if err != nil {
    panic(err)
}
// response: items = page.Records, total = total, optional hasMore = page.HasMore
...
```

`Count` is still a full-range scan. To reduce cost when paging with the same filters:

- Cache `total` by query key such as `bucket + measurement + where + timeRange + field` (do **not** include `Limit` / `Offset`)
- Use a short TTL, or invalidate on write
- If `TimeRange` ends at `now`, align the end timestamp (e.g. to the minute) before building the cache key, or hit rate will be low
- If the UI only needs “next page”, prefer `page.HasMore` and skip `Count`

### Write

#### Create InfluxDB Write Point

```go
...
import influxdb2 "github.com/influxdata/influxdb-client-go/v2"
...
writePoint, err := influxdb2.NewPoint("iot_state", map[string]string{
  "deviceId": "xxxxxxxxxxxx",
}, map[string]any{
  "x": 0,
  "y": 0,
  "yaw": 0,
}, time.Now())
...
```

Or use the method provided by the helper

```go
...
type Location struct {
  X float64 `json:"x" writePoint:"x,field"`
  Y float64 `json:"y" writePoint:"y,field"`
  Yaw float64 `json:"yaw" writePoint:"yaw,field"`
}
type State struct {
  DeviceId string `json:"deviceId" writePoint:"deviceId,tag"`
  Location *Location `json:"location"`
  Timestamp int64 `json:"timestamp" writePoint:",time"`
}

data := `{"deviceId":"xxxxxxxx", "location":{"x": 0, "y": 0, "yaw": 0}, "timestamp": 1721117718000}`

state := &State{}
err := json.Unmarshal([]byte(data), state)
if err != nil {
  panic(err)
}

writePoint, err := idbHelper.ParseStructToWritePoint("iot_state", state)
if err != nil {
  panic(err)
}
```

#### Write To InfluxDB

```go
...
err := helper.Write(context.Background(), writePoint)
if err != nil {
  panic(err)
}
...
```

Or use a custom method to write directly

```go
type Location struct {
  X float64 `json:"x"`
  Y float64 `json:"y"`
  Yaw float64 `json:"yaw"`
}
type State struct {
  DeviceId string `json:"deviceId"`
  Location *Location `json:"location"`
  Timestamp int64 `json:"timestamp"`
}

func writePointGenerator(data any, measurement string) (*write.Point, error) {
  state, ok := data.(*State)
  if !ok {
    return nil, fmt.Errorf("invalid data")
  }
  ts := time.UnixMilli(state.Timestamp)
  tags := map[string]string{
    "deviceId": state.DeviceId,
  }
  fields := map[string]any{
    "x": state.Location.X,
    "y": state.Location.Y,
    "yaw": state.Location.Yaw,
  }
  
  return influxdb2.NewPoint(measurement, tags, fields, ts), nil
}

data := &State{
  DeviceId: "xxxxxxxx",
  Location: &Location{
    X: 0,
    Y: 0,
    Yaw: 0,
  },
  Timestamp: time.Now().UnixMilli(),
}

_, err := helper.WriteByGenerator(context.Background(), "iot_state", data, writePointGenerator)
if err != nil {
  panic(err)
}
```

### Complete Example

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	idbHelper "github.com/zhangsq-ax/influxdb2-helper"
	"os"
	"time"
)

type Location struct {
	X   float64 `json:"x" writePoint:"x,field"`
	Y   float64 `json:"y" writePoint:"y,field"`
	Yaw float64 `json:"yaw" writePoint:"yaw,field"`
}
type State struct {
	DeviceId  string    `json:"deviceId" writePoint:"deviceId,tag"`
	Location  *Location `json:"location"`
	Timestamp int64     `json:"timestamp" writePoint:",time"`
}

var (
	stateData = `{"deviceId": "xxxxxxxx", "location": {"x": 0, "y": 0, "yaw": 0}, "timestamp": 0}`
)

func main() {
	opts := &idbHelper.InfluxdbHelperOptions{
		ServerUrl:  os.Getenv("SERVER_URL"),
		Token:      os.Getenv("TOKEN"),
		OrgName:    os.Getenv("ORG_NAME"),
		BucketName: os.Getenv("BUCKET_NAME"),
	}
	helper := idbHelper.NewInfluxdbHelper(opts)

	// write point1 by struct
	point, err := idbHelper.ParseStructToWritePoint("iot_state", newState(""))
	if err != nil {
		panic(err)
	}
	err = helper.Write(context.Background(), point)
	if err != nil {
		panic(err)
	}

	// write point2 by generator
	state := newState("yyyyyyyy")
	_, err = helper.WriteByGenerator(context.Background(), "iot_state", state, func(data any, measurement string) (*write.Point, error) {
		state, ok := data.(*State)
		if !ok {
			return nil, fmt.Errorf("data type error")
		}
		ts := state.Timestamp
		fields := map[string]any{
			"x":   state.Location.X,
			"y":   state.Location.Y,
			"yaw": state.Location.Yaw,
		}
		tags := map[string]string{
			"deviceId": state.DeviceId,
		}

		return influxdb2.NewPoint(measurement, tags, fields, time.UnixMilli(ts)), nil
	})

	time.Sleep(5 * time.Second)

	// query
	queryOpts := &idbHelper.QueryOptions{
		TimeRange:   &[2]int64{time.Now().Add(-1 * time.Hour).UnixMilli(), time.Now().UnixMilli()},
		BucketName:  os.Getenv("BUCKET_NAME"),
		Measurement: "iot_state",
		Columns: []string{
			"_time",
			"deviceId",
			"x",
			"y",
			"yaw",
		},
		Limit:    100,
		DescSort: true,
	}
	result, err := helper.Query(context.Background(), queryOpts.String())
	if err != nil {
		panic(err)
	}
	for result.Next() {
		fmt.Println(result.Record().Values())
	}
}

func newState(deviceId string) *State {
	state := &State{}
	_ = json.Unmarshal([]byte(stateData), state)
	state.Timestamp = time.Now().UnixMilli()
	if deviceId != "" {
		state.DeviceId = deviceId
	}

	return state
}

```

