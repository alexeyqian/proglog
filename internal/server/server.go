package server

import(
	"context"

	api "github.com/alexeyqian/proglog/api/v1"
	"google.golang.org/gprc"
)

var _api.LogServer = (*grpcServer)(nil)

type CommitLog interface{
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

// create a gRPC server, and register our service to the server.
func NewGRPCServer(config *Config) (*grpc.Server, error){
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config)(srv *grpcServer, err error){
	srv = grpcServer{
		Config: config,
	}

	return &srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) 
	(*api.ProduceResponse, error){
		offset, err := s.CommitLog.Append(req.Record)
		if err != nil {
			return nil, err
		}

		return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest)
	(*api.ConsumeResponse, error)){
		record, err := s.CommitLog.Read(req.Offset)
		if err != nil {
			return nil, err
		}

		return &api.ConsumeResponse{Record: record}, nil
}

// bidirectional streaming RPC
func (s *grpcServer) ProduceStream(steam api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// server-side streaming RPC, server will stream every record that follows offset
// even records that are not in the log yet. when the server reaches the end of the log
// the erver will wait until some appends a record to the log and then continue streaming records to client.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <- stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOffRange:
				continue
			default:
				return err
				
			}

			if err = stream.Send(res); err != nil {
				return err
			}

			req.Offset++
		}
	}
}
