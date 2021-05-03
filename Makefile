compile:
    protoc api/v1/*.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --proto_path=. 

#protoc -I=$SRC_DIR --go_out=$DST_DIR $SRC_DIR/addressbook.proto
