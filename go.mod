module github.com/alexeyqian/proglog

go 1.13

require (
	github.com/casbin/casbin v1.9.1
	github.com/cloudflare/cfssl v1.4.1 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/raft v1.1.1 // indirect
	github.com/hashicorp/serf v0.8.5
	github.com/stretchr/testify v1.7.0
	github.com/tysontate/gommap v0.0.0-20201017170033-6edfc905bae0
	go.opencensus.io v0.22.2
	go.uber.org/zap v1.10.0
	google.golang.org/genproto v0.0.0-20200423170343-7949de9c1215
	google.golang.org/grpc v1.32.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.0.0 // indirect
	google.golang.org/protobuf v1.26.0
)

replace github.com/hashicorp/raft-boltdb => github.com/travisjeffery/raft-boltdb v1.0.0
