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
		servers := r.GetConfiguration().Configuration().Servers
		c.JSON(200, Success(servers, "", nil))
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
				TrxID:  param.TrxID,
			}).ToBytes()
			if err != nil {
				c.JSON(500, Success(nil, "unknown error occurred during generating command for raft sync", nil))
			}
			r.Apply(commandBytes, time.Second)
			c.JSON(200, Success(nil, "", nil))
		} else if r.Leader() != "" {
			c.JSON(
				300,
				Fail(
					r.Leader(),
					fmt.Sprintf("requested node is not leader, current leader is [%v]", r.Leader()),
					nil,
				),
			)
		} else {
			// 不是leader 且当前无leader
			c.JSON(300,
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
		var v []byte
		if param.TrxID == 0 { // 未指定事务，开启新事务完成操作
			v = db.Get([]byte(param.Key))
		} else {
			if trx := db.MapTransaction(param.TrxID); trx != nil {
				v = db.Get([]byte(param.Key))
			} else {
				c.JSON(500, Fail(nil, "未找到指定事务", nil))
			}
		}
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

func StartTransactionHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		if r.State().String() == "Leader" {
			commandBytes, err := (&Command{
				OpType: OpTrx,
				Key:    []byte(""),
				Value:  []byte(""),
				TrxID:  0,
			}).ToBytes()
			if err != nil {
				c.JSON(500, Success(nil, "unknown error occurred during generating command for raft sync", nil))
			}
			newTrxChan := r.Apply(commandBytes, time.Second)
			newTrx := newTrxChan.Response()
			c.JSON(200, Success(gin.H{"trx_id": newTrx.(*drifterdb.Transaction).TrxID()}, "", nil))
		} else if r.Leader() != "" {
			c.JSON(
				300,
				Fail(
					r.Leader(),
					fmt.Sprintf("requested node is not leader, current leader is [%v]", r.Leader()),
					nil,
				),
			)
		} else {
			// 不是leader 且当前无leader
			c.JSON(300,
				Fail(nil, "no leader running, please waiting for the selection.", nil))
		}
	}
}

func CommitTransactionHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		if r.State().String() == "Leader" {
			param := TrxParams{}
			err := c.ShouldBindJSON(&param)
			if err != nil {
				c.JSON(401, Fail(nil, "参数格式错误", nil))
				return
			}
			commandBytes, err := (&Command{
				OpType: OpCmt,
				Key:    []byte(""),
				Value:  []byte(""),
				TrxID:  param.TrxID,
			}).ToBytes()
			if err != nil {
				c.JSON(500, Success(nil, "unknown error occurred during generating command for raft sync", nil))
			}
			r.Apply(commandBytes, time.Second)
			c.JSON(200, Success(true, "", nil))
		} else if r.Leader() != "" {
			c.JSON(
				300,
				Fail(
					r.Leader(),
					fmt.Sprintf("requested node is not leader, current leader is [%v]", r.Leader()),
					nil,
				),
			)
		} else {
			// 不是leader 且当前无leader
			c.JSON(300,
				Fail(nil, "no leader running, please waiting for the selection.", nil))
		}
	}
}

func RollbackTransactionHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		if r.State().String() == "Leader" {
			param := TrxParams{}
			err := c.ShouldBindJSON(&param)
			if err != nil {
				c.JSON(401, Fail(nil, "参数格式错误", nil))
				return
			}
			commandBytes, err := (&Command{
				OpType: OpRol,
				Key:    []byte(""),
				Value:  []byte(""),
				TrxID:  param.TrxID,
			}).ToBytes()
			if err != nil {
				c.JSON(500, Success(nil, "unknown error occurred during generating command for raft sync", nil))
			}
			r.Apply(commandBytes, time.Second)
			c.JSON(200, Success(true, "", nil))
		} else if r.Leader() != "" {
			c.JSON(
				300,
				Fail(
					r.Leader(),
					fmt.Sprintf("requested node is not leader, current leader is [%v]", r.Leader()),
					nil,
				),
			)
		} else {
			// 不是leader 且当前无leader
			c.JSON(300,
				Fail(nil, "no leader running, please waiting for the selection.", nil))
		}
	}
}

func DeleteHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		if r.State().String() == "Leader" {
			param := ReqBody{}
			err := c.ShouldBindJSON(&param)
			if err != nil {
				c.JSON(401, Fail(nil, "参数格式错误", nil))
				return
			}

			commandBytes, err := (&Command{
				OpType: OpDel,
				Key:    []byte(param.Key),
				Value:  []byte(""),
				TrxID:  param.TrxID,
			}).ToBytes()
			if err != nil {
				c.JSON(500, Success(nil, "unknown error occurred during generating command for raft sync", nil))
			}
			r.Apply(commandBytes, time.Second)
			c.JSON(200, Success(nil, "", nil))
		} else if r.Leader() != "" {
			c.JSON(
				300,
				Fail(
					r.Leader(),
					fmt.Sprintf("requested node is not leader, current leader is [%v]", r.Leader()),
					nil,
				),
			)
		} else {
			// 不是leader 且当前无leader
			c.JSON(300,
				Fail(nil, "no leader running, please waiting for the selection.", nil))
		}
	}
}

func RangeHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		param := RangeParams{}
		err := c.ShouldBindJSON(&param)
		if err != nil {
			c.JSON(401, Fail(nil, "参数格式错误", nil))
			return
		}
		var v []*drifterdb.Element
		if param.TrxID == 0 { // 未指定事务，开启新事务完成操作
			v = db.Range([]byte(param.StartKey), []byte(param.EndKey), param.Offset, param.Count)
		} else {
			if trx := db.MapTransaction(param.TrxID); trx != nil {
				v = db.Range([]byte(param.StartKey), []byte(param.EndKey), param.Offset, param.Count)
			} else {
				c.JSON(500, Fail(nil, "未找到指定事务", nil))
			}
		}
		if converter, ok := ConverterMap[param.Type]; ok {
			res := make([]*KV, len(v))
			for _, eachElement := range v {
				res = append(res, &KV{
					Key:   string(eachElement.Key().([]byte)),
					Value: converter(eachElement.Value()),
				})
			}
			c.JSON(200, Success(
				res, "", nil,
			))
		} else {
			c.JSON(401, Fail(nil, fmt.Sprintf("requested type [%v] is not supported", param.Type), nil))
		}
	}
}

func LeaderHandler(db drifterdb.BaseDB, r *raft.Raft) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(200, Success(r.Leader(), "", nil))
	}
}