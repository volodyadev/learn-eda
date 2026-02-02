package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	k "github.com/volodyadev/learn-eda/internal/kafka"
)

const (
	topic        = "golang-producer"
	numberOfKeys = 20
)

var address = []string{"10.10.10.100:9092"} // TODO

func main() {
	p, err := k.NewProducer(address)
	if err != nil {
		logrus.Fatal(err)
	}
	keys := generateUUIDString()
	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("Kafka message %d", i)

		// key := fmt.Sprintf("%d", i%3) // murmur2
		key := keys[i%numberOfKeys]

		timestamp := time.Now()

		if err := p.Produce(msg, topic, key, timestamp); err != nil {
			logrus.Error(err)
		}
	}
}

func generateUUIDString() [numberOfKeys]string {
	var uuids [numberOfKeys]string
	for i := 0; i < numberOfKeys; i++ {
		uuids[i] = uuid.NewString()
	}
	return uuids
}
