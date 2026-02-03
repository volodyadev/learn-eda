package kafka

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	// -1
	sessionTimeout  = 6000 // ms, heartbeat
	consumerTimeout = 1000 // ms, consumer
)

type Handler interface {
	HandleMessage(message []byte, offset kafka.Offset) error
}

type Consumer struct {
	consumer *kafka.Consumer
	handler  Handler
	stop     bool
}

func NewConsumer(handler Handler, address []string, topic, consumerGroup string) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		// server
		"bootstrap.servers": strings.Join(address, ","),

		// topic
		"group.id": consumerGroup,

		// session
		"session.timeout.ms": sessionTimeout,

		// commit
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,

		"auto.offset.reset": "largest", // "earliest"
	}
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}

	// SubscribeTopics
	if err = c.Subscribe(topic, nil); err != nil {
		return nil, err
	}
	return &Consumer{consumer: c, handler: handler}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			break
		}
		kafkaMsg, err := c.consumer.ReadMessage(consumerTimeout)

		if err != nil {
			if kerr, ok := err.(kafka.Error); ok {
				if kerr.Code() == kafka.ErrTimedOut {
					continue  // Нормально, сообщений нет
				}
				if kerr.IsFatal() {
					logrus.Errorf("Fatal Kafka error: %v", err)
					return  // Или panic/restart по вашей логике
				}
				logrus.Warnf("Non-fatal Kafka error: %v", err)
				continue
			}
			logrus.Errorf("Unexpected ReadMessage error: %v", err)
			continue
		}

		if kafkaMsg == nil {
			continue
		}
		if err = c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition.Offset); err != nil {
			logrus.Error(err)
			continue
		}
		// store offset
		if _, err = c.consumer.StoreMessage(kafkaMsg); err != nil {
			logrus.Error(err)
			continue
		}
	}
}

func (c *Consumer) Stop() error {
	c.stop = true
	if _, err := c.consumer.Commit(); err != nil {
		return err
	}
	logrus.Infof("Commited offset")
	return c.consumer.Close()
}
