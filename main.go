package main

import (
	"context"
	"flag"
	"fmt"
	pb "github.com/Jille/raft-grpc-example/proto"
	"github.com/LaJunkai/drifterdb"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	// 命令行参数，tcp地址、raftID、raft目录、是否是bootstrap
	myAddr = flag.String("address", "localhost:50051", "TCP host+port for this node")
	raftId = flag.String("raft_id", "nodeA", "Node id used by Raft")
	drifterAddr  = flag.String("http_address", "0.0.0.0:1127", "tcp host+port for db http interface.")
	raftDir       = flag.String("raft_data_dir", "cluster", "Raft data dir")
	raftBootstrap = flag.Bool("bootstrap", false, "Whether to bootstrap the Raft cluster")
)

func main() {
	// 从命令行获取参数
	flag.Parse()

	if *raftId == "" {
		log.Fatalf("flag --raft_id is required")
	}

	ctx := context.Background()
	// split地址ip和端口
	_, port, err := net.SplitHostPort(*myAddr)
	if err != nil {
		log.Fatalf("failed to parse local address (%q): %v", *myAddr, err)
	}
	// 监听tcp端口
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// 实例化wordTracker
	db := drifterdb.OpenDB("db/" + *raftId)
	wt := NewDrifterX(db)
	// 实例化Raft，传入context、id、监听地址、状态机；获取transport manager
	r, tm, err := NewRaft(ctx, *raftId, *myAddr, wt)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}
	//// 创建一个grpc服务器
	s := grpc.NewServer()
	//// 注册服务器，grpc的方法, 改成http服务对外暴露
	pb.RegisterExampleServer(s, &rpcInterface{
		drifterX: wt, // 状态机实例
		raft:     r,  // Raft实例
	})
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example"})
	raftadmin.Register(s, r)
	reflection.Register(s)
	go StartDrifterServer(db, r)
	fmt.Println("after")
	if err := s.Serve(sock); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func StartDrifterServer(db drifterdb.BaseDB, r *raft.Raft) {


	router := gin.Default()
	router.POST("/db/put", PutHandler(db, r))
	router.POST("/db/get", GetHandler(db, r))
	_, port, err := net.SplitHostPort(*drifterAddr)
	if err != nil {
		log.Fatalf("error occurred during parsing http address of the db: %v", err.Error())
	}
	router.Run(":" + port)
}

func NewRaft(ctx context.Context, myID, myAddress string, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(*raftDir, myID)

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	if *raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	}

	return r, tm, nil
}
