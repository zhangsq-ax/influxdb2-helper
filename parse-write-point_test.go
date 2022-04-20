package influxdb2_helper

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseStructToWritePoint(t *testing.T) {
	type Location struct {
		X   float64 `json:"x" writePoint:"x,field"`
		Y   float64 `json:"y" writePoint:"y,field"`
		Yaw float64 `json:"yaw" writePoint:"yaw,field"`
	}
	type Test struct {
		Foo       string    `json:"foo" writePoint:"foo,tag"`
		Bar       string    `json:"bar" writePoint:"bar,field"`
		Location  *Location `json:"location"`
		Timestamp int64     `json:"timestamp" writePoint:",time"`
	}

	ts := time.Now().UnixMilli()
	pt, err := ParseStructToWritePoint("test", &Test{
		Foo: "foo",
		Bar: "bar",
		Location: &Location{
			X: 10,
		},
		Timestamp: ts,
	})
	assert.NoError(t, err)
	t.Log(pt)
	assert.Equal(t, "test", pt.Name())
	assert.Equal(t, "foo", pt.TagList()[0].Key)
	assert.Equal(t, "bar", pt.FieldList()[0].Key)
	assert.Equal(t, "x", pt.FieldList()[1].Key)
	assert.Equal(t, time.UnixMilli(ts), pt.Time())
}

func TestParseStructToWritePoint2(t *testing.T) {
	type TaskInfo struct {
		HasRunNum int                    `json:"hasRunNum"`
		IsCancel  bool                   `json:"isCancel"`
		IsFinish  bool                   `json:"isFinish"`
		IsPaused  bool                   `json:"isPaused"`
		Target    map[string]interface{} `json:"target"`
		TaskId    string                 `json:"taskId" writePoint:"taskId,tag"`
		TaskIndex int                    `json:"taskIndex"`
		TaskType  int                    `json:"taskType"`
		TotalNum  int                    `json:"totalNum"`
	}

	type State struct {
		Battery         int                    `json:"battery" writePoint:"battery,field"`
		BusinessId      string                 `json:"businessId" writePoint:"businessId,tag"`
		AreaId          string                 `json:"floorId" writePoint:"areaId,tag"`
		LocationQuality int                    `json:"locQuality" writePoint:"locQuality,field"`
		X               float64                `json:"x" writePoint:"x,field"`
		Y               float64                `json:"y" writePoint:"y,field"`
		Yaw             float64                `json:"yaw" writePoint:"yaw,field"`
		Speed           float64                `json:"speed" writePoint:"speed,field"`
		HasObstruction  bool                   `json:"hasObstruction"`
		IsCharging      bool                   `json:"isCharging"`
		IsEmergencyStop bool                   `json:"isEmergencyStop"`
		IsGoHome        bool                   `json:"isGoHome"`
		IsManualMode    bool                   `json:"isManualMode"`
		IsRemoteMode    bool                   `json:"isRemoteMode"`
		Disinfect       map[string]interface{} `json:"disinfect"`
		TaskInfo        *TaskInfo              `json:"taskObj,omitempty"`
		Errors          []interface{}          `json:"errors"`
	}

	type RobotState struct {
		DeviceId  string `json:"deviceId" writePoint:"deviceId,tag"`
		Timestamp int64  `json:"timestamp" writePoint:",time"`
		State     *State `json:"state"`
	}

	state := &RobotState{}
	strState := `{"deviceId": "71922044000721a", "timestamp": 1650436924223, "state": {"battery": 0, "businessId": "60d998a1fccc72d6fd363627", "disinfect": {"gearType": 0, "isDryBurned": false}, "dispatch": 0, "distance": 0, "errors": [{"code": 20002, "level": 1, "message": "robot stop for a long time", "type": 2}], "floorId": "60d9a519fccc72d6fd363628", "hasObstruction": false, "isCharging": false, "isEmergencyStop": false, "isGoHome": false, "isManualMode": false, "isRemoteMode": false, "locQuality": 100, "speed": 0, "x": 0.66, "y": -4.61, "yaw": -0.1}}`
	err := json.Unmarshal([]byte(strState), state)
	assert.NoError(t, err)

	pt, err := ParseStructToWritePoint("robot_state", state)
	assert.NoError(t, err)
	pt.AddField("rawData", strState)

	assert.Equal(t, "robot_state", pt.Name())
	t.Log(pt)
	t.Log("----- Tags -----")
	for _, tag := range pt.TagList() {
		t.Log(tag.Key, tag.Value)
	}
	t.Log("----- Fields -----")
	for _, field := range pt.FieldList() {
		t.Log(field.Key, field.Value)
	}
	assert.Equal(t, time.UnixMilli(1650436924223), pt.Time())
}
