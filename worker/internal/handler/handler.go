package handler

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) HandleMessage(message []byte, offset kafka.Offset) error {
	logrus.Infof("Message from kafka offset %d '%s", offset, string(message))
	return nil
}
