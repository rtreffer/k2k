package main

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

type StaticPartitioner struct {
	hash int32
}

func (p *StaticPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	return p.hash % numPartitions, nil
}

func (p *StaticPartitioner) RequiresConsistency() bool {
	return true
}

func NewStaticPartitioner(hash int32) sarama.Partitioner {
	return &StaticPartitioner{hash}
}

func NewKafkaBaseConfig(keyHash int32, latencyTarget time.Duration) (*sarama.Config, error) {
	partitioner := NewStaticPartitioner(keyHash)

	cfg := sarama.NewConfig()
	cfg.ClientID = "kernel" + strconv.Itoa((rand.Int()%900000)+100000)
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionZSTD
	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner { return partitioner }
	cfg.Producer.Flush.Frequency = latencyTarget
	cfg.Producer.Flush.Bytes = cfg.Producer.MaxMessageBytes
	cfg.Producer.Flush.Messages = cfg.Producer.MaxMessageBytes
	cfg.Metadata.Full = false
	cfg.ChannelBufferSize = 1
	cfg.Version = sarama.V2_1_0_0

	return cfg, cfg.Validate()
}
