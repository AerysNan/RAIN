package main

import (
	"context"
	pm "rain/proto/manager"
	pw "rain/proto/worker"
	"time"

	"github.com/sirupsen/logrus"
)

type Worker struct {
	pw.WorkerForManagerServer
	client pm.ManagerForWorkerClient
	index  int64
}

func New(client pm.ManagerForWorkerClient) *Worker {
	worker := &Worker{
		client: client,
	}
	go worker.SendHeartbeat()
	return worker
}

func (w *Worker) SendHeartbeat() {
	timer := time.NewTicker(time.Second)
	for {
		<-timer.C
		_, err := w.client.Heartbeat(context.Background(), &pm.HeartbeatRequest{})
		if err != nil {
			logrus.WithError(err).Error("Send heartbeat failed")
		}
	}
}

func (w *Worker) Put(ctx context.Context, request *pw.PutRequest) (*pw.PutResponse, error) {
	return &pw.PutResponse{}, nil
}

func (w *Worker) Get(ctx context.Context, request *pw.GetRequest) (*pw.GetResponse, error) {
	return &pw.GetResponse{}, nil
}
