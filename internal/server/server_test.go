package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/alexeyqian/proglog/api/v1"
	"github.com/alexeyqian/proglog/internal/log"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T){
	funcsMap := range​ ​map​[​string​]​func​(
		​ 	                t *testing.T,
		​ 	                client api.LogClient,
		​ 	                config *Config,
		​ 	        ){
		​ 	                ​"produce/consume a message to/from the log succeeeds"​:
		​ 	                        testProduceConsume,
		​ 	                ​"produce/consume stream succeeds"​:
		​ 	                        testProduceConsumeStream,
		​ 	                ​"consume past log boundary fails"​:
		​ 	                        testConsumePastBoundary,
		​ 	        }

	for scenario, fn := range funcsMap{
		t.Run(scenario, func(t *testing.T){
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}

	func setupTest(t *testing.T, fn func(*Config))(
		client api.LogClient,
		cfg *Config,
		tearndown func(),
	){

		t.Helper()

		// port :0 means auto assign a free port
		listen, err := net.listen("tcp", ":0")
		require.NoError(t, err)

		clientOptions := []grpc.DialOption{grpc.WithInsecure()}
		cc, err := grpc.Dial(listen.Addr().String(), clientOptions...)
		require.NoError(t, err)

		dir, err := ioutil.TempDir("", "server-test")
		require.NoError(t, err)

		clog, err := log.NewLog(dir, log.Config{})
		require.NoError(t, err)

		cfg = Config{
			CommitLog: clog,
		}

		if fn != nil {
			fn(&cfg)
		}

		server, err := NewGRPCServer(cfg)
		require.NoError(t, err)

		go func(){
			// Serve is a blocking call, has to run in go routine
			// otherwise any code below it wouldn't able to run.
			server.Serve(listen) 
		}()

		client = api.NewLogClient(cc)

		return client, &cfg, func(){
			server.Stop()
			cc.Close()
			listen.Close()
			clog.Remove()
		}
	}

	func testProduceConsume(t *testing.T, client api.LogClient, config *Config){
		ctx := context.Background()

		want := api.Record{
			Value: []byte("hello world")
		}

		prequest := api.ProduceRequest{
			Record: &want
		}

		produce, err := client.Produce(ctx, &prequest)
		require.NoError(t, err)

		crequest := api.ConsumeRequest{
			Offset: produce.Offset
		}
		consume, err := client.Consume(ctx, &crequest)
		require.NoError(t, err)
		require.Equal(t, want.Value, consume.Record.Value)
		require.Equal(t, want.Offset, consume.Record.Offset)\
	}

	func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config){
		ctx := context.Back()

		record := api.Record{
			Value: []byte("hello world")
		}
		prequest := api.ProduceRequest{
			Record: &record
		}
		produce, err := client.Produce(ctx, &prequest)
		require.NoError(t, err)

		crequest := api.ConsumeRequest{
			Offset: produce.Offset + 1
		}
		consume, err := client.Consume(ctx, &crequest)
		if consume != nil {
			t.Fatal("consume not nil")
		}

		got := grpc.Code(err)
		want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
		if got != want {
			t.Fatalf("got err:%v, want: %v", got, want)
		}
	}

	func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config){
		ctx := context.Background()

		records := []*api.Rrecord{
			{
				Value: []byte("first message")
				Offset: 0,
			},{
				Value: []byte("second message"),
				Offset: 1,
			}
		}

		// code block

		{
			stream, err := client.ProduceStream(ctx)
			require.NoError(t, err)

			for offset, record := range records {
				err = stream.Send(&api.ProduceRequest{
					Record: record,
				})

				require.NoError(t, err)
				
				res, err := stream.Recv()
				require.NoError(t, err)
				if res.Offset != int64(offset){
					t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
				}
			}

		}

		// code block
		{
			stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
			require.NoError(t, err)

			for i, record := range records {
				res, err := stream.Recv()
				require.NoError(t, err)
				require.Equal(t, res.Record, &api.Record{
					Value: record.Value,
					Offset: int64(i)
				})
			}
		}
	}