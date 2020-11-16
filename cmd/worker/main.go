package main

import (
	"fmt"
	"net"
	pm "rain/proto/manager"
	pw "rain/proto/worker"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	local   = kingpin.Flag("local", "Local address").Short('l').Default("127.0.0.1").String()
	address = kingpin.Flag("address", "Address of manager server").Short('a').Default("127.0.0.1:8080").String()
	port    = kingpin.Flag("port", "Listen port of worker server").Short('p').Default("8081").String()
)

func main() {
	kingpin.Parse()
	listenAddress := fmt.Sprintf("%s:%s", "0.0.0.0", *port)
	listen, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	server := grpc.NewServer()
	connection, err := grpc.Dial(*address, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Fatal("Connect to manager server failed")
	}
	defer connection.Close()
	client := pm.NewManagerForWorkerClient(connection)
	pw.RegisterWorkerForManagerServer(server, New(client, fmt.Sprintf("%s:%s", *local, *port)))

	logrus.WithField("address", listenAddress).Info("Server started")
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
