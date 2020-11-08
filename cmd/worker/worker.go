package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	pm "rain/proto/manager"
	pw "rain/proto/worker"
	"time"

	"github.com/sirupsen/logrus"
)

type worker struct {
	pw.WorkerForManagerServer

	client    pm.ManagerForWorkerClient
	local     string
	id        int
	size      int
	blocksize int
}

func new(client pm.ManagerForWorkerClient, local string) *worker {
	w := &worker{
		client:    client,
		local:     local,
		blocksize: 100,
		id:        -1,
	}
	path := fmt.Sprintf("data/%s", local)
	_ = os.Mkdir(path, os.ModePerm)
	go w.SendHeartbeat()
	return w
}

func (w *worker) SendHeartbeat() {
	timer := time.NewTicker(time.Second)
	for {
		response, err := w.client.Heartbeat(context.Background(), &pm.HeartbeatRequest{
			Address: w.local,
			Id:      int64(w.id),
		})
		if err != nil {
			logrus.WithError(err).Error("Send heartbeat failed")
			<-timer.C
			continue
		}
		w.id = int(response.Id)
		<-timer.C
	}
}

func (w *worker) Put(ctx context.Context, request *pw.PutRequest) (*pw.PutResponse, error) {
	logrus.WithField("size", len(request.Value)).Info("Receive put request")
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, uint64(len(request.Value)))
	currentSize, offset := w.size, w.size
	if request.Offset >= 0 {
		currentSize = int(request.Offset)
	}
	currentBlock, value := currentSize/w.blocksize, append(header, request.Value...)
	for {
		remain := (currentBlock+1)*w.blocksize - currentSize
		if remain >= len(value) {
			break
		}
		file, err := os.OpenFile(fmt.Sprintf("data/%s/%d", w.local, currentBlock), os.O_CREATE|os.O_WRONLY, 0777)
		defer file.Close()
		if err != nil {
			logrus.WithError(err).Error("Open file to write failed")
			return nil, err
		}
		_, err = file.WriteAt(value[:remain], int64(w.blocksize-remain))
		if err != nil {
			logrus.WithError(err).Error("Open file to write failed")
			return nil, err
		}
		value = value[remain:]
		currentSize += remain
		currentBlock++
	}
	file, err := os.OpenFile(fmt.Sprintf("data/%s/%d", w.local, currentBlock), os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()
	if err != nil {
		logrus.WithError(err).Error("Open file to write failed")
		return nil, err
	}
	currentSize += len(value)
	_, err = file.Write(value)
	if err != nil {
		logrus.WithError(err).Error("Write file failed")
		return nil, err
	}
	if request.Offset < 0 {
		w.size = currentSize
	}
	return &pw.PutResponse{
		Offset: int64(offset),
	}, nil
}

func (w *worker) Get(ctx context.Context, request *pw.GetRequest) (*pw.GetResponse, error) {
	offset := int(request.Offset)
	size, err := w.ReadHeader(offset)
	if err != nil {
		logrus.WithError(err).Error("Read header failed")
		return nil, err
	}
	logrus.WithField("size", size).Info("Receive get request")
	value := make([]byte, 0)
	currentBlock, blockOffset := (offset+8)/w.blocksize, (offset+8)%w.blocksize
	for {
		remain := w.blocksize - blockOffset
		if size < remain {
			break
		}
		file, err := os.Open(fmt.Sprintf("data/%s/%d", w.local, currentBlock))
		defer file.Close()
		if err != nil {
			logrus.WithError(err).Error("Open file to read failed")
			return nil, err
		}
		chunk := make([]byte, remain)
		_, err = file.ReadAt(chunk, int64(blockOffset))
		if err != nil {
			logrus.WithError(err).Error("Read file failed")
			return nil, err
		}
		value = append(value, chunk...)
		currentBlock++
		blockOffset = 0
		size -= remain
	}
	file, err := os.Open(fmt.Sprintf("data/%s/%d", w.local, currentBlock))
	defer file.Close()
	if err != nil {
		logrus.WithError(err).Error("Open file to read failed")
		return nil, err
	}
	chunk := make([]byte, size)
	if _, err := file.Read(chunk); err != nil {
		logrus.WithError(err).Error("Read file failed")
		return nil, err
	}
	value = append(value, chunk...)
	return &pw.GetResponse{
		Value: value,
	}, nil
}

func (w *worker) ReadHeader(offset int) (int, error) {
	currentBlock, blockOffset := offset/w.blocksize, offset%w.blocksize
	file, err := os.Open(fmt.Sprintf("data/%s/%d", w.local, currentBlock))
	defer file.Close()
	if err != nil {
		logrus.WithError(err).Error("Open file to read failed")
		return -1, err
	}
	header := make([]byte, 8)
	if w.blocksize-blockOffset >= 8 {
		_, err = file.ReadAt(header, int64(blockOffset))
		if err != nil {
			logrus.WithError(err).Error("Read file header failed")
			return -1, err
		}
	} else {
		firstChunk := make([]byte, w.blocksize-blockOffset)
		_, err = file.ReadAt(firstChunk, int64(blockOffset))
		if err != nil {
			logrus.WithError(err).Error("Read file header failed")
			return -1, err
		}
		currentBlock++
		file, err = os.Open(fmt.Sprintf("data/%s/%d", w.local, currentBlock))
		defer file.Close()
		if err != nil {
			logrus.WithError(err).Error("Open file to read failed")
			return -1, err
		}
		secondChunk := make([]byte, 8-len(firstChunk))
		_, err := file.Read(secondChunk)
		if err != nil {
			logrus.WithError(err).Error("Read file failed")
			return -1, err
		}
		copy(header[:len(firstChunk)], firstChunk)
		copy(header[len(firstChunk):], secondChunk)
	}
	return int(binary.LittleEndian.Uint64(header)), nil
}
