package kafkaemployees

import "google.golang.org/protobuf/proto"

type ProtoEvent interface {
	proto.Message
	GetEvent() *Event
}
