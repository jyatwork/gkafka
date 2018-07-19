//create by jyatwork
//by 2018.03.25

package kafka

import (
	"conf_yaml/conf" //配合

	kafka "github.com/Shopify/sarama"
	"github.com/alecthomas/log4go"
	"github.com/bsm/sarama-cluster" //support automatic consumer-group rebalancing and offset tracking
)

type Kafka struct {
	producer kafka.SyncProducer
}

var kfk *Kafka

type SolidPartitioner struct {
}

func (p *SolidPartitioner) Partition(message *kafka.ProducerMessage, numPartitions int32) (int32, error) {
	return message.Partition, nil
}

func (p *SolidPartitioner) RequiresConsistency() bool {
	return false
}

func Producer() *Kafka {
	if kfk == nil {
		config := kafka.NewConfig()
		//config.Producer.Partitioner = kafka.NewRandomPartitioner
		config.Producer.Return.Successes = true

		config.Producer.Partitioner = func(topic string) kafka.Partitioner {
			pner := &SolidPartitioner{}
			return pner
		}
		brokerList := conf.StringList("kafka.broker-list")

		producer, err := kafka.NewSyncProducer(brokerList, config)
		if err != nil {
			log4go.Error("Failed to create producer: %s.", err.Error())
			return nil
		}

		kfk = &Kafka{producer: producer}

		return kfk
	}
	return kfk
}

// syncProducer 同步生产者
// 并发量小时，可以用这种方式
//noinspection ALL
func (p *Kafka) Produce(data string) error {

	//是否启用推送
	enable := conf.Bool("kafka.enable")
	if enable == false {
		log4go.Info("kafka product enable: false")
		return nil
	}

	var (
		partition int32
	)

	topic := conf.String("kafka.topic")
	key := conf.String("kafka.key")

	partition = 1
	msg := &kafka.ProducerMessage{Topic: topic, Key: kafka.StringEncoder(key), Value: kafka.StringEncoder(data), Partition: partition}
	//fmt.Println("partition assigned:", msg.Partition)
	parti, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		log4go.Error("Failed to produce message to kafka cluster: %s.", err.Error())
		return err
	}

	//fmt.Printf("Produced message:%s to partition %d with offset %d\n", data, parti, offset)

	log4go.Debug("Produced message:%s to partition %d with offset %d", data, parti, offset)

	return nil
}

// kafka Consumer 消费者
func Consumer() {
	groupID := conf.String("kafka.groupid") //指明consumer所在的组，如果多个地方都使用相同的groupid，可能造成个别消费者消费不到的情况
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	//config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = kafka.OffsetNewest //初始从最新的offset开始
	brokerList := conf.StringList("kafka.broker-list")
	topic := conf.String("kafka.topic")
	topics := []string{topic}
	c, err := cluster.NewConsumer(brokerList, groupID, topics, config)
	if err != nil {
		log4go.Error("Failed open consumer: %v", err)
		return
	}
	defer c.Close()
	go func(c *cluster.Consumer) {
		errors := c.Errors()
		noti := c.Notifications()
		for {
			select {
			case err := <-errors:
				log4go.Error(err)
			case <-noti:
			}
		}
	}(c)

	for msg := range c.Messages() {
		log4go.Info("[topic:%s] [partition:%d] [offset:%d] \n msg:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		c.MarkOffset(msg, "") //MarkOffset 并不是实时写入kafka: 可能在程序crash时丢掉未提交的offset
	}
}

// asyncProducer 异步生产者
// 并发量大时，必须采用这种方式
func asyncProducer(data string) {
	topic := conf.String("kafka.topic")
	key := conf.String("kafka.key")
	brokerList := conf.StringList("kafka.broker-list")
	config := kafka.NewConfig()
	config.Producer.Return.Successes = true //必须有这个选项
	//config.Producer.Timeout = 5 * time.Second
	p, err := kafka.NewAsyncProducer(brokerList, config)
	defer p.Close()
	if err != nil {
		log4go.Error("kafka Consumer:", err)
		return
	}

	//必须有这个匿名函数内容
	go func(p kafka.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					log4go.Error("kafka AsyncProducer:", err)
				}
			case <-success:
			}
		}
	}(p)

	msg := &kafka.ProducerMessage{
		Topic: topic,
		Key:   kafka.StringEncoder(key),
		Value: kafka.ByteEncoder(data),
	}
	log4go.Info("Consume message:%+v", msg)
	p.Input() <- msg
}
