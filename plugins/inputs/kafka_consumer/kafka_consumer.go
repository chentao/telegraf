package kafka_consumer

import (
	"log"
	"strings"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

type Kafka struct {
	ConsumerGroup string
	Topics        []string
	BrokerList    []string
	Consumer      *cluster.Consumer

	// Legacy metric buffer support
	MetricBuffer int
	// TODO remove PointBuffer, legacy support
	PointBuffer int

	Offset string
	parser parsers.Parser

	sync.Mutex

	// channel for all incoming kafka messages
	in <-chan *sarama.ConsumerMessage
	// channel for all kafka consumer errors
	errs <-chan error
	done chan struct{}

	// keep the accumulator internally:
	acc telegraf.Accumulator

	// doNotCommitMsgs tells the parser not to call CommitUpTo on the consumer
	// this is mostly for test purposes, but there may be a use-case for it later.
	doNotCommitMsgs bool
}

var sampleConfig = `
  ## topic(s) to consume
  topics = ["telegraf"]
  ## an array of broker connection strings
  broker_list = ["localhost:9093"]
  ## the name of the consumer group
  consumer_group = "telegraf_metrics_consumers"
  ## Offset (must be either "oldest" or "newest")
  offset = "oldest"

  ## Data format to consume.
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`

func (k *Kafka) SampleConfig() string {
	return sampleConfig
}

func (k *Kafka) Description() string {
	return "Read metrics from Kafka topic(s)"
}

func (k *Kafka) SetParser(parser parsers.Parser) {
	k.parser = parser
}

func (k *Kafka) Start(acc telegraf.Accumulator) error {
	k.Lock()
	defer k.Unlock()

	k.acc = acc

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	switch strings.ToLower(k.Offset) {
	case "oldest", "":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		log.Printf("I! WARNING: Kafka consumer invalid offset '%s', using 'oldest'\n",
			k.Offset)
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	var err error
	k.Consumer, err = cluster.NewConsumer(k.BrokerList, k.ConsumerGroup, k.Topics, config)
	if err != nil {
		return err
	}

	k.in = k.Consumer.Messages()
	k.errs = k.Consumer.Errors()

	k.done = make(chan struct{})

	// Start the kafka message reader
	go k.receiver()
	log.Printf("I! Started the kafka consumer service, peers: %v, topics: %v\n",
		k.BrokerList, k.Topics)
	return nil
}

// receiver() reads all incoming messages from the consumer, and parses them into
// influxdb metric points.
func (k *Kafka) receiver() {
	for {
		select {
		case <-k.done:
			return
		case err := <-k.errs:
			if err != nil {
				log.Printf("E! Kafka Consumer Error: %s\n", err)
			}
		case msg := <-k.in:
			metrics, err := k.parser.Parse(msg.Value)
			if err != nil {
				log.Printf("E! Kafka Message Parse Error\nmessage: %s\nerror: %s",
					string(msg.Value), err.Error())
			}

			for _, metric := range metrics {
				k.acc.AddFields(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
			}

			if !k.doNotCommitMsgs {
				k.Lock()
				k.Consumer.MarkOffset(msg, "")
				k.Unlock()
			}
		}
	}
}

func (k *Kafka) Stop() {
	k.Lock()
	defer k.Unlock()
	close(k.done)
	if err := k.Consumer.Close(); err != nil {
		log.Printf("E! Error closing kafka consumer: %s\n", err.Error())
	}
}

func (k *Kafka) Gather(acc telegraf.Accumulator) error {
	return nil
}

func init() {
	inputs.Add("kafka_consumer", func() telegraf.Input {
		return &Kafka{}
	})
}
