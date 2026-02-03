package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/volodyadev/learn-eda/internal/handler"
	"github.com/volodyadev/learn-eda/internal/kafka"
)

var address = []string{"10.10.10.100:9092"} // TODO

const (
	topic         = "golang-producer"
	consumerGroup = "consumer-group"
)

func main() {
	h := handler.NewHandler()
	c, err := kafka.NewConsumer(h, address, topic, consumerGroup)
	if err != nil {
		logrus.Fatal(err)
	}

	// go func(){
	// 	c.Start()
	// }()

	go c.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logrus.Fatal(c.Stop())

}
