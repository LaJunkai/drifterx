package main

import (
	"fmt"
	"github.com/LaJunkai/drifterdb"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"time"
)

func MachinesHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {

	}
}

func PutHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 判断leader状态
		if r.State().String() == "Leader" {
			param := ReqBody{}
			err := c.ShouldBindJSON(&param)
			if err != nil {
				c.JSON(401, Fail(nil, "参数格式错误", nil))
				return
			}
			convertedValue, err := ConvertToBytes(param.Type, param.Value)
			if err != nil {
				c.JSON(401, Fail(nil, err.Error(), nil))
				return
			}
			commandBytes, err := (&Command{
				OpType: OpPut,
				Key:    []byte(param.Key),
				Value:  convertedValue,
				TrxID:  0,
			}).ToBytes()
			if err != nil {
				c.JSON(500, Success(nil, "unknown error occurred during generating command for raft sync", nil))
			}
			r.Apply(commandBytes, time.Second)
			db.Put([]byte(param.Key), convertedValue)
			c.JSON(200, Success(nil, "", nil))
		} else if r.Leader() != "" {
			c.JSON(
				300,
				Fail(
					gin.H{},
					fmt.Sprintf("requested node is not leader, current leader is [%v]", r.Leader()),
					nil,
				),
			)
		} else {
			// 不是leader 且当前无leader
			c.JSON(500,
				Fail(nil, "no leader running, please waiting for the selection.", nil))
		}

	}
}

func GetHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		param := ReqBody{}
		err := c.ShouldBindJSON(&param)
		if err != nil {
			c.JSON(401, Fail(nil, "参数格式错误", nil))
			return
		}
		v := db.Get([]byte(param.Key))

		if converter, ok := ConverterMap[param.Type]; ok {
			c.JSON(200, Success(
				gin.H{
					"key":   param.Key,
					"value": converter(v),
				}, "", nil,
			))
		} else {
			c.JSON(401, Fail(nil, fmt.Sprintf("requested type [%v] is not supported", param.Type), nil))
		}
	}
}
