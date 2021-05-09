package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

var ConverterMap = map[string]func([]byte) interface{} {
	"int": func(b []byte) interface{} {
		return int(binary.BigEndian.Uint64(b))
	},
	"string": func(b []byte) interface{} {
		return string(b)
	},
	"bool": func(b []byte) interface{} {
		return string(b) == "1"
	},
	"json": func(b []byte) interface{} {
		var mapResult map[string]interface{}
		json.Unmarshal(b, &mapResult)
		return mapResult
	},
}

func ConvertToBytes(t string, data interface{}) ([]byte, error) {
	if t == "int" {
		result := make([]byte, 8)
		binary.BigEndian.PutUint64(result, uint64(int(data.(float64))))
		return result, nil
	} else if t == "string" {
		return []byte(data.(string)), nil
	} else if t == "bool" {
		if boolValue := data.(bool); boolValue {
			return []byte("1"), nil
		} else {
			return []byte("0"), nil
		}
	} else if t == "json" {
		return json.Marshal(data)
	} else {
		return nil, errors.New(fmt.Sprintf("unsupported type [%v]", t))
	}
}
