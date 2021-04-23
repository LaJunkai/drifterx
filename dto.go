package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
)

type Command struct {
	OpType int `json:"op_type"`
	Key []byte `json:"key"`
	Value []byte `json:"value"`
	TrxID uint32 `json:"trx_id"`
}

func (c *Command) ToBytes() ([]byte, error) {
	return json.Marshal(c)
}

func Success(data interface{}, message string, details interface{}) map[string]interface{} {
	return gin.H{
		"success": true,
		"data": data,
		"message": message,
		"details": details,
	}
}

func Fail(data interface{}, message string, details interface{}) map[string]interface{} {
	return gin.H{
		"success": false,
		"data": data,
		"message": message,
		"details": details,
	}
}

type ReqBody struct {
	Key string `json:"key"`
	Value interface{} `json:"value"`
	Type string `json:"type"` // int string bool json
	TrxID uint32 `json:"trx_id"`
}