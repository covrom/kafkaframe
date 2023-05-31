package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/covrom/kafkaframe"
	"github.com/covrom/kafkaframe/examples/producer/hr"
)

func main() {
	var cfg Config
	err := cfg.Parse("./config.yaml")
	if err != nil {
		log.Fatal("config error: ", err)
	}
	kfk := kafkaframe.NewKafka(cfg.Kafka)
	ut := hr.NewUsersTask(kfk)

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	if err := ut.ExecuteSync(ctx); err != nil {
		log.Fatal("ExecuteSync failed:", err)
	}
}
