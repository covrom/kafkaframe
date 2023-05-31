package kafkaframe

import (
	"context"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

type TestMessage struct {
	Name string
}

func TestKafka_SendTopic(t *testing.T) {
	if os.Getenv("CI_JOB_NAME") != "" {
		// skip test in CI pipeline
		return
	}
	cf, err := os.Open("./test_cfg.yaml")
	if err != nil {
		t.Error(err)
		return
	}
	defer cf.Close()
	dec := yaml.NewDecoder(cf)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		t.Error(err)
		return
	}
	k := NewKafka(cfg)
	tp, err := k.OpenTopic("test")
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := tp.SendJSON(context.Background(), TestMessage{
		Name: "test name of object",
	}); err != nil {
		t.Error(err)
		return
	}
	tp.Close()
}

func TestKafka_ReceiveTopic(t *testing.T) {
	if os.Getenv("CI_JOB_NAME") != "" {
		// skip test in CI pipeline
		return
	}
	cf, err := os.Open("./test_cfg.yaml")
	if err != nil {
		t.Error(err)
		return
	}
	defer cf.Close()
	dec := yaml.NewDecoder(cf)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		t.Error(err)
		return
	}
	k := NewKafka(cfg)
	ts, err := k.OpenSubscription("testgroup", "test")
	if err != nil {
		t.Error(err)
		return
	}
	var tm TestMessage
	msg, err := ts.ReceiveJSON(context.Background(), &tm)
	if err != nil {
		t.Error(err)
		return
	}
	msg.Ack()

	ts.Close()
}
