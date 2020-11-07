package main

import (
	"context"
	pm "rain/proto/manager"
	pw "rain/proto/worker"

	"rain/rs"
)

type Manager struct {
	pm.ManagerForClientServer
	pm.ManagerForWorkerServer

	encoder *rs.ReedSolomon
	clients []pw.WorkerForManagerClient
}

func New(dataShard int) (*Manager, error) {
	rs, err := rs.New(dataShard)
	if err != nil {
		return nil, err
	}
	return &Manager{
		encoder: rs,
		clients: make([]pw.WorkerForManagerClient, 0),
	}, nil

}

func (m *Manager) Write(ctx context.Context, request *pm.WriteRequest) (*pm.WriteResponse, error) {
	return &pm.WriteResponse{}, nil
}

func (m *Manager) Read(ctx context.Context, request *pm.ReadRequest) (*pm.ReadResponse, error) {
	return &pm.ReadResponse{}, nil
}

func (m *Manager) Heartbeat(ctx context.Context, request *pm.HeartbeatRequest) (*pm.HeartbeatResponse, error) {
	return &pm.HeartbeatResponse{}, nil
}
