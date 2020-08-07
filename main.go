package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

type clientMetadata struct {
	clientId      string
	extraLabels   map[string]string
	latencyTarget time.Duration
	broker        string
	partitionKey  string
	topic         string
}

type stringify func(*klogmessage) string

func jsonify(meta clientMetadata) stringify {
	prefix := "{"
	suffix := "}"

	if meta.extraLabels != nil && len(meta.extraLabels) > 0 {
		for key, value := range meta.extraLabels {
			prefix = prefix + "\"" + key + "\":\"" + value + "\","
		}
	}

	return func(msg *klogmessage) string {
		metadata := ""
		if msg.labels != nil && len(msg.labels) > 0 {
			metadata = `,"metadata":[`
			for i, meta := range msg.labels {
				if i > 0 {
					metadata = metadata + ","
				}
				metadata = metadata + `"` + meta + `"`
			}
			metadata = metadata + "]"
		}
		timestamp, _ := json.Marshal(msg.readTimestamp)
		return prefix +
			`"seq":` + strconv.Itoa(msg.seq) + `,` +
			`"facility":"` + msg.facility + `",` +
			`"severity":"` + msg.severity + `",` +
			`"message":"` + strings.ReplaceAll(msg.message, `"`, `\"`) + `",` +
			`"timestamp":` + string(timestamp) +
			metadata +
			suffix
	}
}

func main() {
	meta := clientMetadata{}

	// parse command line flags
	flag.DurationVar(&meta.latencyTarget, "kafka.latency", 100*time.Millisecond, "latency target for message batching, e.g. 100ms.")
	flag.StringVar(&meta.clientId, "kafka.clientId", "", "the kafka client id - must be unique per producer. Defaults to kernel + random")
	flag.StringVar(&meta.broker, "kafka.broker", "", "the kafka broker(s) to connect to")
	flag.StringVar(&meta.topic, "kafka.topic", "kernel", "the kafka topic, defaults to kernel")
	flag.StringVar(&meta.partitionKey, "kafka.partitionKey", "", "the key to determine the patition where all kernel logs should go")
	rawExtraLabels := flag.String("labels", "", "extra labels to put into the json message payload, e.g. hostname=10-12-8-1,kernel_version=5.8.12")
	stdout := flag.Bool("stdout", false, "duplicate messages to stdout")

	help := flag.Bool("help", false, "print the help message")

	flag.Parse()

	if !flag.Parsed() || (help != nil && *help) {
		flag.PrintDefaults()
		os.Exit(0)
	}
	if meta.broker == "" {
		flag.PrintDefaults()
		fmt.Printf("\n--kafka.broker required, got %v\n", os.Args[1:])
		os.Exit(1)
	}

	// copy extra labels
	if rawExtraLabels != nil && *rawExtraLabels != "" {
		meta.extraLabels = make(map[string]string)
		for _, label := range strings.Split(*rawExtraLabels, ",") {
			if label == "" {
				continue
			}
			keyValue := strings.SplitN(label, "=", 2)
			if len(keyValue) != 2 {
				fmt.Printf("invalid key/value pair: %v", label)
				os.Exit(1)
			}
			meta.extraLabels[keyValue[0]] = keyValue[1]
		}
	}

	// setup kernel log fetching
	log := make(chan *klogmessage, 100)
	go func() {
		panic(klog(log))
	}()

	// set up kafka client
	hash := fnv.New32a()
	if meta.partitionKey == "" {
		utsname := &syscall.Utsname{}
		if err := syscall.Uname(utsname); err != nil {
			panic(err)
		}
		nodename := make([]byte, 0, 65)
		for _, b := range utsname.Nodename {
			if b == 0 {
				break
			}
			nodename = append(nodename, byte(b))
		}
		meta.partitionKey = string(nodename)
	} else {
		hash.Write([]byte(meta.partitionKey))
	}
	kafkaCfg, err := NewKafkaBaseConfig(int32(hash.Sum32()), meta.latencyTarget)
	if err != nil {
		panic(err)
	}
	producer, err := sarama.NewAsyncProducer(strings.Split(meta.broker, ","), kafkaCfg)
	if err != nil {
		panic(err)
	}
	kafkaOutput := producer.Input()

	go func() {
		for _ = range producer.Successes() {}
	}()
	go func() {
		for err := range producer.Errors() {
			panic(err)
		}
	}()

	// process logs
	jsonf := jsonify(meta)
	for msg := range log {
		rawMessage := jsonf(msg)
		if stdout != nil && *stdout {
			fmt.Printf("klog: %v\n      %v\n", msg, rawMessage)
		}
		kafkaOutput <- &sarama.ProducerMessage {
			Topic: meta.topic,
			Value: sarama.StringEncoder(rawMessage),
			Timestamp: msg.readTimestamp,
		}
	}
}
