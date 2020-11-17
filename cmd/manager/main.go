package main

import (
	"fmt"
	"net"
	pm "rain/proto/manager"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	port  = kingpin.Flag("port", "Listen port of manager server").Short('p').Default("8080").String()
	shard = kingpin.Flag("shard", "Number of data shards").Short('s').Default("4").Int()
)

func main() {
	kingpin.Parse()
	listenAddress := fmt.Sprintf("%s:%s", "0.0.0.0", *port)
	listen, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	manager, err := new(*shard)
	if err != nil {
		logrus.WithError(err).Fatal("Create manager server failed")
	}
	server := grpc.NewServer()
	pm.RegisterManagerForClientServer(server, manager)
	pm.RegisterManagerForWorkerServer(server, manager)
	logrus.WithField("address", listenAddress).Info("Server started")
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
