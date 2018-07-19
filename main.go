package main

import (
	"conf_yaml/conf"
	"gkafka/kafka"
)

func main() {
	conf.Load("conf.yaml")
	//kafka.Producer().Produce("it is a demo.")
	kafka.Consumer()
}
