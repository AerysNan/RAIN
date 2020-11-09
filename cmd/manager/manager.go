package main

import (
	"context"
	pm "rain/proto/manager"
	pw "rain/proto/worker"

	"rain/rs"

	"github.com/sirupsen/logrus"
)

type Manager struct {
	pm.ManagerForClientServer
	pm.ManagerForWorkerServer

	id      int64
	encoder *rs.ReedSolomon
	files   map[string]File
	clients map[int64]pw.WorkerForManagerClient
}

type File struct {
	id      int64
	offsets []int64
}

func New(dataShard int) (*Manager, error) {
	rs, err := rs.New(dataShard)
	if err != nil {
		return nil, err
	}
	return &Manager{
		id:      0,
		encoder: rs,
		clients: make(map[int64]pw.WorkerForManagerClient, 0),
	}, nil
}

func (m *Manager) Write(ctx context.Context, request *pm.WriteRequest) (*pm.WriteResponse, error) {
	return &pm.WriteResponse{}, nil
}

func (m *Manager) Read(ctx context.Context, request *pm.ReadRequest) (*pm.ReadResponse, error) {
	return &pm.ReadResponse{}, nil
}

func (m *Manager) Heartbeat(ctx context.Context, request *pm.HeartbeatRequest) (*pm.HeartbeatResponse, error) {
	logrus.Info("Receive heartbeat")
	return &pm.HeartbeatResponse{}, nil
}
