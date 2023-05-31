//go:generate protoc -I ./ --go_out=../kafkaemployees/ --go_opt=paths=source_relative kafka.proto
package api

// Install this tools before run 'go generate':
// go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
