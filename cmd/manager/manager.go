package main

import (
	"context"
	pm "rain/proto/manager"
	pw "rain/proto/worker"
	"sync"

	"rain/rs"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Manager struct {
	pm.ManagerForClientServer
	pm.ManagerForWorkerServer

	id        int
	dataShard int
	encoder   *rs.ReedSolomon
	files     map[string]File
	clients   map[int]pw.WorkerForManagerClient
}

type File struct {
	id      int
	size    int
	offsets []int
}

func New(dataShard int) (*Manager, error) {
	rs, err := rs.New(dataShard)
	if err != nil {
		return nil, err
	}
	return &Manager{
		id:        0,
		dataShard: dataShard,
		encoder:   rs,
		files:     make(map[string]File, 0),
		clients:   make(map[int]pw.WorkerForManagerClient, 0),
	}, nil
}

func (m *Manager) Write(ctx context.Context, request *pm.WriteRequest) (*pm.WriteResponse, error) {
	file := File{
		id:      m.id,
		size:    len(request.Value),
		offsets: make([]int, m.dataShard+2),
	}
	dataShards := m.encoder.Split([]byte(request.Value))
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(m.dataShard)
	for i := 0; i < m.dataShard; i++ {
		go func(i int) {
			defer waitGroup.Done()
			client := m.clients[(i+file.id)%(m.dataShard+2)]
			response, err := client.Put(context.Background(), &pw.PutRequest{
				Value: dataShards[i],
			})
			if err != nil {
				// TODO
				logrus.WithError(err).Error("Put failed")
			}
			file.offsets[i] = int(response.Offset)
		}(i)
	}
	waitGroup.Wait()
	PShard, QShard, err := m.encoder.Encode(dataShards)
	if err != nil {
		return nil, err
	}
	response, err := m.clients[(file.id+m.dataShard)%(m.dataShard+2)].Put(context.Background(), &pw.PutRequest{
		Value: PShard,
	})
	if err != nil {
		return nil, err
	}
	file.offsets[m.dataShard] = int(response.Offset)
	response, err = m.clients[(file.id+m.dataShard+1)%(m.dataShard+2)].Put(context.Background(), &pw.PutRequest{
		Value: QShard,
	})
	if err != nil {
		return nil, err
	}
	file.offsets[m.dataShard+1] = int(response.Offset)
	m.files[request.Key] = file
	m.id += 1
	return &pm.WriteResponse{}, nil
}

func (m *Manager) Read(ctx context.Context, request *pm.ReadRequest) (*pm.ReadResponse, error) {
	file := m.files[request.Key]
	dataShards := make([][]byte, m.dataShard)
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(m.dataShard)
	errChannel := make(chan error)
	for i := 0; i < m.dataShard; i++ {
		go func(i int) {
			defer waitGroup.Done()
			client := m.clients[(i+file.id)%m.dataShard]
			response, err := client.Get(context.Background(), &pw.GetRequest{
				Offset: int64(file.offsets[i]),
			})
			if err != nil {
				logrus.WithError(err).Errorf("Read content from worker %v failed", i)
				errChannel <- err
				return
			}
			dataShards[i] = []byte(response.Value)
		}(i)
	}
	waitGroup.Wait()
	if len(errChannel) > 0 {
		return nil, <-errChannel
	}
	content := m.encoder.Merge(dataShards)
	return &pm.ReadResponse{
		Value: content[:file.size],
	}, nil
}

func (m *Manager) Heartbeat(ctx context.Context, request *pm.HeartbeatRequest) (*pm.HeartbeatResponse, error) {
	address := request.Address
	connection, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Fatal("Connect to manager server failed")
		return nil, err
	}
	client := pw.NewWorkerForManagerClient(connection)
	if request.Id < 0 {
		logrus.Info("Receive new worker")
		id := len(m.clients)
		m.clients[id] = client
		return &pm.HeartbeatResponse{
			Id: int64(id),
		}, nil
	}
	return &pm.HeartbeatResponse{
		Id: request.Id,
	}, nil
}
