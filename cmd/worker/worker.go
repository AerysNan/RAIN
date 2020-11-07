package main

import (
	"context"
	pm "rain/proto/manager"
	pw "rain/proto/worker"
)

type Worker struct {
	pw.WorkerForManagerServer
	client pm.ManagerForWorkerClient
}

func New(client pm.ManagerForWorkerClient) *Worker {
	return &Worker{
		client: client,
	}
}

func (w *Worker) Put(ctx context.Context, request *pw.PutRequest) (*pw.PutResponse, error) {
	return &pw.PutResponse{}, nil
}

func (w *Worker) Get(ctx context.Context, request *pw.GetRequest) (*pw.GetResponse, error) {
	return &pw.GetResponse{}, nil
}
