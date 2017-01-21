package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

type JSONParser struct {
	MetricName  string
	TagKeys     []string
	ExtractKeys []string
	DefaultTags map[string]string
}

func (p *JSONParser) parseArray(buf []byte) ([]telegraf.Metric, error) {
	metrics := make([]telegraf.Metric, 0)

	var jsonOut []map[string]interface{}
	err := json.Unmarshal(buf, &jsonOut)
	if err != nil {
		err = fmt.Errorf("unable to parse out as JSON Array, %s", err)
		return nil, err
	}
	for _, item := range jsonOut {
		metrics, err = p.parseObject(metrics, item)
	}
	return metrics, nil
}

func (p *JSONParser) parseObject(metrics []telegraf.Metric, jsonOut map[string]interface{}) ([]telegraf.Metric, error) {

	tags := make(map[string]string)
	for k, v := range p.DefaultTags {
		tags[k] = v
	}

	for _, tag := range p.TagKeys {
		switch v := jsonOut[tag].(type) {
		case string:
			tags[tag] = v
		case bool:
			tags[tag] = strconv.FormatBool(v)
		case float64:
			tags[tag] = strconv.FormatFloat(v, 'f', -1, 64)
		}
		delete(jsonOut, tag)
	}

	f := JSONFlattener{}

	if len(p.ExtractKeys) > 0 {
		extract_keys_map := make(map[string]int)
		for _, k := range p.ExtractKeys {
			extract_keys_map[k] = 1
		}
		f.ExtractKeysMap = extract_keys_map
	}

	err := f.FlattenJSON("", jsonOut)
	if err != nil {
		return nil, err
	}

	metric, err := metric.New(p.MetricName, tags, f.Fields, time.Now().UTC())

	if err != nil {
		return nil, err
	}
	return append(metrics, metric), nil
}

func (p *JSONParser) Parse(buf []byte) ([]telegraf.Metric, error) {

	if !isarray(buf) {
		metrics := make([]telegraf.Metric, 0)
		var jsonOut map[string]interface{}
		err := json.Unmarshal(buf, &jsonOut)
		if err != nil {
			err = fmt.Errorf("unable to parse out as JSON, %s", err)
			return nil, err
		}
		return p.parseObject(metrics, jsonOut)
	}
	return p.parseArray(buf)
}

func (p *JSONParser) ParseLine(line string) (telegraf.Metric, error) {
	metrics, err := p.Parse([]byte(line + "\n"))

	if err != nil {
		return nil, err
	}

	if len(metrics) < 1 {
		return nil, fmt.Errorf("Can not parse the line: %s, for data format: influx ", line)
	}

	return metrics[0], nil
}

func (p *JSONParser) SetDefaultTags(tags map[string]string) {
	p.DefaultTags = tags
}

type JSONFlattener struct {
	Fields         map[string]interface{}
	ExtractKeysMap map[string]int
}

// FlattenJSON flattens nested maps/interfaces into a fields map (ignoring bools and string)
func (f *JSONFlattener) FlattenJSON(
	fieldname string,
	v interface{}) error {
	if f.Fields == nil {
		f.Fields = make(map[string]interface{})
	}
	return f.FullFlattenJSON(fieldname, "", v, false, false)
}

// FullFlattenJSON flattens nested maps/interfaces into a fields map (including bools and string)
func (f *JSONFlattener) FullFlattenJSON(
	fieldname string,
	k string,
	v interface{},
	convertString bool,
	convertBool bool,
) error {
	if f.Fields == nil {
		f.Fields = make(map[string]interface{})
	}
	fieldname = strings.Trim(fieldname, "_")
	switch t := v.(type) {
	case map[string]interface{}:
		for k, v := range t {
			err := f.FullFlattenJSON(fieldname+"_"+k+"_", k, v, convertString, convertBool)
			if err != nil {
				return err
			}
		}
	case []interface{}:
		for i, v := range t {
			k := strconv.Itoa(i)
			err := f.FullFlattenJSON(fieldname+"_"+k+"_", k, v, convertString, convertBool)
			if err != nil {
				return nil
			}
		}
	case float64:
		f.Fields[fieldname] = t
	case string:
		if _, ok := f.ExtractKeysMap[k]; ok {
			fv, err := strconv.ParseFloat(v.(string), 64)
			if err != nil {
				return nil
			}
			f.Fields[fieldname] = fv
			return nil
		}
		if convertString {
			f.Fields[fieldname] = v.(string)
		} else {
			return nil
		}
	case bool:
		if _, ok := f.ExtractKeysMap[k]; ok {
			var bv float64 = 0
			if v.(bool) == true {
				bv = 1
			}
			f.Fields[fieldname] = bv
			return nil
		}
		if convertBool {
			f.Fields[fieldname] = v.(bool)
		} else {
			return nil
		}
	case nil:
		// ignored types
		fmt.Println("json parser ignoring " + fieldname)
		return nil
	default:
		return fmt.Errorf("JSON Flattener: got unexpected type %T with value %v (%s)",
			t, t, fieldname)
	}
	return nil
}

func isarray(buf []byte) bool {
	ia := bytes.IndexByte(buf, '[')
	ib := bytes.IndexByte(buf, '{')
	if ia > -1 && ia < ib {
		return true
	} else {
		return false
	}
}
