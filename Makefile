compile:
    protoc api/v1/log.proto --go_out=. --proto_path=. --go_opt=paths=source_relative

test:
    go test -race ./...