package main

import (
	"context"
	"encoding/json"
	"errors"
	pb "github.com/Jille/raft-grpc-example/proto"
	"github.com/Jille/raft-grpc-leader-rpc/rafterrors"
	"github.com/LaJunkai/drifterdb"
	"github.com/hashicorp/raft"
	"io"
	"log"
	"time"
)

const (
	OpPut = iota
	OpDel = iota
	OpGet = iota
	OpRol = iota
	OpCmt = iota
	OpTrx = iota // 新开事务
)

// DrifterX keeps track of the three longest words it ever saw.
type DrifterX struct {
	db drifterdb.BaseDB
}

func NewDrifterX(db drifterdb.BaseDB) *DrifterX {
	return &DrifterX{db: db}
}


var _ raft.FSM = &DrifterX{}



func LoadCommandFromBytes(b []byte) *Command {
	c := &Command{}
	err := json.Unmarshal(b, c)
	if err != nil {
		log.Fatalf("unable to parse json command %v", string(b))
	}
	return c
}

func (x *DrifterX) Apply(l *raft.Log) interface{} {
	c := LoadCommandFromBytes(l.Data)
	switch c.OpType {
	case OpPut:
		{
			if c.TrxID == 0 { // 未指定事务，开启新事务完成操作
				return x.db.Put(c.Key, c.Value)
			} else {
				if trx := x.db.MapTransaction(c.TrxID); trx != nil {
					return trx.Put(c.Key, c.Value)
				} else {
					return errors.New("未找到指定事务，执行失败")
				}
			}
		}
	case OpDel:
		if c.TrxID == 0 { // 未指定事务，开启新事务完成操作
			return x.db.Delete(c.Key)
		} else {
			if trx := x.db.MapTransaction(c.TrxID); trx != nil {
				return trx.Delete(c.Key)
			} else {
				return errors.New("未找到指定事务，执行失败")
			}
		}

	case OpRol:
		x.db.RollbackTransactionByID(c.TrxID)
	case OpCmt:
		x.db.CommitTransactionByID(c.TrxID)
	case OpTrx:
		return x.db.StartTransaction()
	}
	return nil
}

func (x *DrifterX) Snapshot() (raft.FSMSnapshot, error) {
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &snapshot{}, nil
}

func (x *DrifterX) Restore(r io.ReadCloser) error {
	//b, err := ioutil.ReadAll(r)
	//if err != nil {
	//	return err
	//}
	//words := strings.Split(string(b), "\n")
	//copy(f.words[:], words)
	return nil
}

type snapshot struct {

}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	//_, err := sink.Write([]byte(strings.Join(s.words, "\n")))
	//if err != nil {
	//	sink.Cancel()
	//	return fmt.Errorf("sink.Write(): %v", err)
	//}
	return sink.Close()
}

func (s *snapshot) Release() {
}


type rpcInterface struct {
	drifterX *DrifterX
	raft     *raft.Raft
}

func (r rpcInterface) AddWord(ctx context.Context, req *pb.AddWordRequest) (*pb.AddWordResponse, error) {
	if r.raft.State() == raft.Leader {

	}
	f := r.raft.Apply([]byte(req.GetWord()), time.Second)
	if err := f.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}
	return &pb.AddWordResponse{}, nil
}

func (r rpcInterface) GetWords(ctx context.Context, req *pb.GetWordsRequest) (*pb.GetWordsResponse, error) {
	// TODO: These two should be read under a mutex to handle concurrent Apply() calls.
	return &pb.GetWordsResponse{}, nil
}
