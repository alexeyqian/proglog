CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init:
    mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
    cfssl gencert \ 
    -initca certs/ca-csr.json | cfssljson -bare ca

    cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=certs/ca-config.json \
    -profile=server \
    certs/server-csr.json | cfssljson -bare server
    mv *.pem *.csr ${CONFIG_PATH}

    cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=certs/ca-config.json \
    -profile=client \
    certs/client-csr.json|cfssljson -bare client


.PHONY: compile
compile:
    ${GOBIN}/protoc api/v1/*.proto \
    --go_out=. \
    --go-grpc_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    --proto_path=. 

.PHONY: test
test:
    go test -race ./...

#protoc -I=$SRC_DIR --go_out=$DST_DIR $SRC_DIR/addressbook.proto
#export PATH="$PATH:$(go env GOPATH)/bin"
#protoc --go_out=. --go_opt=paths=source_relative \
#    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
#    helloworld/helloworld.proto