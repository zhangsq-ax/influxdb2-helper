package influxdb2_helper

import (
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"reflect"
	"strings"
	"time"
)

// ParseStructToWritePoint 将 Struct 解析为 InfluxDB 的 write.Point
func ParseStructToWritePoint(measurement string, s interface{}) (*write.Point, error) {
	v := reflect.ValueOf(s)
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid struct")
	}

	point := influxdb2.NewPoint(measurement, map[string]string{}, map[string]interface{}{}, time.Now())

	err := parseStructToPoint(point, t, v)
	return point, err
}

func parseStructToPoint(pt *write.Point, t reflect.Type, v reflect.Value) error {
	if !v.IsValid() {
		return nil
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		// 判断是不是 Struct 类型
		ft := field.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
			value = value.Elem()
		}
		if ft.Kind() == reflect.Struct {
			err := parseStructToPoint(pt, ft, value)
			if err != nil {
				return err
			}
			//} else if !value.IsZero() {
		} else {
			if fieldName, fieldType, ok := parseTag(field.Tag); ok {
				switch fieldType {
				case "field": // influxdb Fields
					pt.AddField(fieldName, value.Interface())
				case "tag": // influxdb Tags
					if ft.Kind() == reflect.String {
						pt.AddTag(fieldName, value.Interface().(string))
					} else {
						return fmt.Errorf("invalid tag type: %v", field.Name)
					}
				case "time": // influxdb Time
					switch field.Type.Kind() {
					case reflect.Int64:
						pt.SetTime(time.UnixMilli(value.Int()))
					case reflect.Struct:
						if field.Type.String() == "time.Time" {
							pt.SetTime(value.Interface().(time.Time))
						}
					default:
						return fmt.Errorf("invalid time type: %v", field.Name)
					}
				}
			}
		}
	}

	return nil
}

func parseTag(tag reflect.StructTag) (fieldName string, fieldType string, ok bool) {
	var tagContent string
	if tagContent, ok = tag.Lookup("writePoint"); ok {
		r := strings.Split(tagContent, ",")
		if len(r) > 1 {
			fieldName = r[0]
			fieldType = r[1]
			return fieldName, fieldType, true
		} else {
			return "", "", false
		}
	} else {
		return "", "", false
	}
}
