package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/covrom/kafkaframe"
	"github.com/covrom/kafkaframe/examples/consumer/kfkempl"
)

func main() {
	var cfg Config
	err := cfg.Parse("./config.yaml")
	if err != nil {
		log.Fatal("config error: ", err)
	}
	kfk := kafkaframe.NewKafka(cfg.Kafka)

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	_ = kfkempl.NewUsersSubscribe(ctx, kfk)

	<-ctx.Done()
}
