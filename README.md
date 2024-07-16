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
    Columns: []string{"_time", "deviceId", "..."},
    Limit: 100,
    Offset: 0,
    DescSort: true,
}
...
```

or 

```go
...
queryOpts := helper.NewQueryOptions("iot_state", map[string]string{}, ["_time", "deviceId", "..."], 1721059200000, 1721106016000, 100, 0)
...
```

> **TimeRange** - Required. The time range for querying data, UTC timestamp in milliseconds
>
> **BucketName** - Required. The name of bucket to query
>
> **Measurement** - Required. The name of measurement to query
>
> **Where** - Optional. Tag-based query and filter conditions. Currently only the "and" relationship is supported between multiple conditions. By Default, no conditional filtering is performed.
>
> **Columns** - Optional. Columns returned by the query result. By default, all columns are returned.
>
> **Limit** - Optional. Return the limit of query result records. By default there is no limit on the number of records returned, but be aware that this may have performance issues.
>
> **Offset** - Optinal. The number of records to skip when returning query results. Used together with the **Limit** parameter to implement query result paging.
>
> **DescSort** - Optional. Whether to sort the query results in reverse order based on time.

#### Query Data

```go
...
result, err := helper.Query(context.Background(), queryOpts.String())
if err != nil {
  panic(err)
}
for result.Next() {
  fmt.Println(result.Record().Values())
}
...
```

or

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

import(
  idbHelper "github.com
)
```

