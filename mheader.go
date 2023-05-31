package kafkaframe

import (
	"time"

	"github.com/rs/xid"
)

const idField = "id"

type MessageHeader struct {
	ID       string `json:"id"`
	Topic    string `json:"topic"`
	Source   string `json:"source"`   // source system (clientIdPrefix prefix from config)
	Producer string `json:"producer"` // SaramaCfg.ClientID
}

func (m MessageHeader) toMap() map[string]string {
	return map[string]string{
		idField:     m.ID,
		"topic":     m.Topic,
		"source":    m.Source,
		"producer":  m.Producer,
		"createdAt": m.CreatedAt().Format(time.RFC3339Nano), // for human reading
	}
}

func (m *MessageHeader) fromMap(src map[string]string) {
	m.ID = src[idField]
	m.Topic = src["topic"]
	m.Source = src["source"]
	m.Producer = src["producer"]
}

func (m MessageHeader) CreatedAt() time.Time {
	guid, err := xid.FromString(m.ID)
	if err != nil {
		return time.Time{}
	}
	return guid.Time()
}

func (m MessageHeader) Xid() xid.ID {
	guid, err := xid.FromString(m.ID)
	if err != nil {
		return xid.NilID()
	}
	return guid
}
