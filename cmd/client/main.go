package main

import (
	"context"
	"io/ioutil"
	"os"
	pm "rain/proto/manager"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app     = kingpin.New("client", "RAIN client")
	address = app.Flag("address", "Address of manager server").Short('a').Default("0.0.0.0:8080").String()

	writeFile         = app.Command("write", "Write file")
	writeFileFlagKey  = writeFile.Flag("key", "File key").Short('k').Required().String()
	writeFileFlagPath = writeFile.Flag("path", "File path").Short('p').Required().String()

	readFile         = app.Command("read", "Read file")
	readFileFlagKey  = readFile.Flag("key", "File key").Short('k').Required().String()
	readFileFlagName = readFile.Flag("path", "File name").Short('n').Required().String()
)

func main() {
	command, err := app.Parse(os.Args[1:])
	if err != nil {
		logrus.WithError(err).Fatal("Parse command failed")
	}
	connection, err := grpc.Dial(*address, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Fatal("Connect to manager server failed")
	}
	defer connection.Close()
	client := pm.NewManagerForClientClient(connection)
	switch command {
	case writeFile.FullCommand():
		file, err := os.Open(*writeFileFlagPath)
		if err != nil {
			logrus.WithError(err).Fatal("Open input file failed")
		}
		defer file.Close()
		content, err := ioutil.ReadAll(file)
		if err != nil {
			logrus.WithError(err).Fatal("Read input file failed")
		}
		_, err = client.Write(context.Background(), &pm.WriteRequest{
			Key:   *writeFileFlagKey,
			Value: string(content),
		})
		if err != nil {
			logrus.WithError(err).Fatal("Write input file to remote failed")
		}
		logrus.Info("Write finished")
	case readFile.FullCommand():
		response, err := client.Read(context.Background(), &pm.ReadRequest{
			Key: *readFileFlagKey,
		})
		if err != nil {
			logrus.WithError(err).Fatal("Read remote file failed")
		}
		file, err := os.OpenFile(*readFileFlagName, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			logrus.WithError(err).Fatal("Open output file failed")
		}
		defer file.Close()
		_, err = file.Write([]byte(response.Value))
		if err != nil {
			logrus.WithError(err).Fatal("Write output file failed")
		}
		logrus.Info("Read finish")
	}
}
