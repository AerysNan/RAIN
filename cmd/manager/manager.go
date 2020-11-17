package main

import (
	"context"
	"errors"
	pm "rain/proto/manager"
	pw "rain/proto/worker"
	"sync"

	"rain/rs"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	errInternal      = errors.New("internal error")
	errDuplicateKey  = errors.New("duplicate key")
	errNoSuckKey     = errors.New("no such key")
	errRecoverFailed = errors.New("failed to write recover data")
)

type manager struct {
	pm.ManagerForClientServer
	pm.ManagerForWorkerServer

	id        int
	dataShard int
	encoder   *rs.ReedSolomon
	files     map[string]file
	clients   map[int]pw.WorkerForManagerClient
}

type file struct {
	id      int
	size    int
	offsets []int
}

func new(dataShard int) (*manager, error) {
	rs, err := rs.New(dataShard)
	if err != nil {
		return nil, err
	}
	return &manager{
		id:        0,
		dataShard: dataShard,
		encoder:   rs,
		files:     make(map[string]file, 0),
		clients:   make(map[int]pw.WorkerForManagerClient, 0),
	}, nil
}

func (m *manager) Write(ctx context.Context, request *pm.WriteRequest) (*pm.WriteResponse, error) {
	if _, ok := m.files[request.Key]; ok {
		return nil, errDuplicateKey
	}
	f := file{
		id:      m.id,
		size:    len(request.Value),
		offsets: make([]int, m.dataShard+2),
	}
	dataShards := m.encoder.Split([]byte(request.Value))
	PShard, QShard, err := m.encoder.Encode(dataShards)
	if err != nil {
		return nil, err
	}
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(m.dataShard + 2)
	errChannel := make(chan error, (m.dataShard+2)*3)
	// 3 consecutive put failed, then write failed
	for i := 0; i < m.dataShard; i++ {
		go func(i int) {
			defer waitGroup.Done()
			for j := 1; j <= 3; j++ {
				client := m.clients[(i+f.id)%(m.dataShard+2)]
				response, err := client.Put(context.Background(), &pw.PutRequest{
					Value:  dataShards[i],
					Offset: -1,
				})
				if err != nil {
					errChannel <- err
					logrus.WithError(err).Errorf("Put %v shard failed, tried %v time(s)", i, j)
					continue
				}
				f.offsets[i] = int(response.Offset)
				break
			}
		}(i)
	}
	go func() {
		defer waitGroup.Done()
		for j := 1; j <= 3; j++ {
			response, err := m.clients[(f.id+m.dataShard)%(m.dataShard+2)].Put(context.Background(), &pw.PutRequest{
				Value:  PShard,
				Offset: -1,
			})
			if err != nil {
				errChannel <- err
				logrus.WithError(err).Errorf("Put P shard failed, tried %v time(s)", j)
				continue
			}
			f.offsets[m.dataShard] = int(response.Offset)
			break
		}
	}()
	go func() {
		defer waitGroup.Done()
		for j := 1; j <= 3; j++ {
			response, err := m.clients[(f.id+m.dataShard+1)%(m.dataShard+2)].Put(context.Background(), &pw.PutRequest{
				Value:  QShard,
				Offset: -1,
			})
			if err != nil {
				errChannel <- err
				logrus.WithError(err).Errorf("Put Q shard failed, tried %v time(s)", j)
				continue
			}
			f.offsets[m.dataShard+1] = int(response.Offset)
			break
		}
	}()
	waitGroup.Wait()
	if len(errChannel) > 0 {
		return nil, errInternal
	}
	m.files[request.Key] = f
	m.id++
	return &pm.WriteResponse{}, nil
}

func (m *manager) Read(ctx context.Context, request *pm.ReadRequest) (*pm.ReadResponse, error) {
	f, ok := m.files[request.Key]
	if !ok {
		return nil, errNoSuckKey
	}
	dataShards := make([][]byte, m.dataShard)
	var PShard, QShard []byte
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(m.dataShard + 2)
	for i := 0; i < m.dataShard; i++ {
		go func(i int) {
			defer waitGroup.Done()
			for j := 1; j <= 3; j++ {
				client := m.clients[(i+f.id)%(m.dataShard+2)]
				response, err := client.Get(context.Background(), &pw.GetRequest{
					Offset: int64(f.offsets[i]),
				})
				if err != nil {
					logrus.WithError(err).Errorf("Read %v shard failed, tried %v time(s)", i, j)
					continue
				}
				dataShards[i] = []byte(response.Value)
				break
			}
		}(i)
	}
	go func() {
		defer waitGroup.Done()
		for j := 1; j <= 3; j++ {
			client := m.clients[(f.id+m.dataShard)%(m.dataShard+2)]
			response, err := client.Get(context.Background(), &pw.GetRequest{
				Offset: int64(f.offsets[m.dataShard]),
			})
			if err != nil {
				logrus.WithError(err).Errorf("Read P shard failed, tried %v time(s)", j)
				continue
			}
			PShard = []byte(response.Value)
			break
		}
	}()

	go func() {
		defer waitGroup.Done()
		for j := 1; j <= 3; j++ {
			client := m.clients[(f.id+m.dataShard+1)%(m.dataShard+2)]
			response, err := client.Get(context.Background(), &pw.GetRequest{
				Offset: int64(f.offsets[m.dataShard+1]),
			})
			if err != nil {
				logrus.WithError(err).Errorf("Read Q shard failed, tried %v time(s)", j)
				continue
			}
			QShard = []byte(response.Value)
			break
		}
	}()
	waitGroup.Wait()

	indices := make([]int, 0)
	for i, dataShard := range dataShards {
		if dataShard == nil {
			indices = append(indices, i)
		}
	}
	recoverP, recoverQ, err := m.encoder.Recover(dataShards, PShard, QShard)
	if err != nil {
		return nil, err
	}
	go m.rewrite(dataShards, indices, recoverP, recoverQ, f)
	content := m.encoder.Merge(dataShards)
	return &pm.ReadResponse{
		Value: content[:f.size],
	}, nil
}

func (m *manager) rewrite(dataShards [][]byte, indices []int, recoverP []byte, recoverQ []byte, f file) {
	total := len(indices)
	if recoverP != nil {
		total++
	}
	if recoverQ != nil {
		total++
	}
	if total == 0 {
		logrus.Info("No need to rewrite data")
		return
	}
	errChannel := make(chan error, total)
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(total)
	for _, index := range indices {
		go func(i int) {
			defer waitGroup.Done()
			client := m.clients[(f.id+i)%(m.dataShard+2)]
			_, err := client.Put(context.Background(), &pw.PutRequest{
				Value:  dataShards[i],
				Offset: int64(f.offsets[i]),
			})
			if err != nil {
				logrus.WithError(err).Errorf("Rewrite %v shard failed", i)
				errChannel <- err
				return
			}
		}(index)
	}
	if recoverP != nil {
		go func() {
			defer waitGroup.Done()
			client := m.clients[(f.id+m.dataShard)%(m.dataShard+2)]
			_, err := client.Put(context.Background(), &pw.PutRequest{
				Value:  recoverP,
				Offset: int64(f.offsets[m.dataShard]),
			})
			if err != nil {
				logrus.WithError(err).Error("Rewrite P shard failed")
				errChannel <- err
				return
			}
		}()
	}
	if recoverQ != nil {
		go func() {
			defer waitGroup.Done()
			client := m.clients[(f.id+m.dataShard+1)%(m.dataShard+2)]
			_, err := client.Put(context.Background(), &pw.PutRequest{
				Value:  recoverQ,
				Offset: int64(f.offsets[m.dataShard+1]),
			})
			if err != nil {
				logrus.WithError(err).Error("Rewrite Q shard failed")
				errChannel <- err
				return
			}
		}()
	}
	waitGroup.Wait()
	if len(errChannel) > 0 {
		logrus.Error("Rewrite data failed")
	} else {
		logrus.Info("Rewrite data succeeded")
	}
}

func (m *manager) Heartbeat(ctx context.Context, request *pm.HeartbeatRequest) (*pm.HeartbeatResponse, error) {
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
