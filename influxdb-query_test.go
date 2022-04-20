package influxdb2_helper

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueryOptions_String(t *testing.T) {
	opts := &QueryOptions{
		TimeRange:   &[2]int64{1650363467735, 1650363506374},
		BucketName:  "iot_state",
		Measurement: "robot_state",
		Where: map[string]string{
			"deviceId": "71922044000721a",
		},
		Fields: []string{"rawData"},
	}

	str, err := opts.String()
	assert.NoError(t, err)
	t.Log(str)
}
