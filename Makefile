CERT_SOURCE=./certs
CONFIG_PATH=${HOME}/.proglog/

init:
	mkdir -p ${CONFIG_PATH}

gencert: 
	${GOBIN}/cfssl gencert -initca ${CERT_SOURCE}/ca-csr.json | cfssljson -bare ca
	
	${GOBIN}/cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=${CERT_SOURCE}/ca-config.json \
	-profile=server \
	${CERT_SOURCE}/server-csr.json | cfssljson -bare server

	${GOBIN}/cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=${CERT_SOURCE}/ca-config.json \
	-profile=client \
	-cn="root" \
	${CERT_SOURCE}/client-csr.json|cfssljson -bare root-client

	${GOBIN}/cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=${CERT_SOURCE}/ca-config.json \
	-profile=client \
	-cn="nobody" \
	${CERT_SOURCE}/client-csr.json|cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

proto: 
	/usr/local/bin/protoc ./api/v1/*.proto \
	--go_out=. \
	--go-grpc_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_opt=paths=source_relative \
	--proto_path=. 


$(CONFIG_PATH)/model.conf:
	cp ${CERT_SOURCE}/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp ${CERT_SOURCE}/policy.csv $(CONFIG_PATH)/policy.csv

test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	go test -race ./...