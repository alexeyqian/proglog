package server

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"

	api "github.com/alexeyqian/proglog/api/v1"
	"github.com/alexeyqian/proglog/internal/auth"
	configx "github.com/alexeyqian/proglog/internal/config"
	"github.com/alexeyqian/proglog/internal/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable obervability for debugging")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

type fn func(*testing.T, api.LogClient, api.LogClient, *Config)

func TestServer(t *testing.T) {

	funcsMap := map[string]fn{
		"produce and consume":        testProduceConsume,
		"produce and consume stream": testProduceConsumeStream,
		"consume past log boundary":  testConsumePastBoundary,
		"unauthorized fails":         testUnauthorized,
	}

	for scenario, fn := range funcsMap {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	config *Config,
	tearndown func(),
) {

	t.Helper()

	// 1 setup server

	// 1.1 setup commit log
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	commitLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(configx.ACLModelFile, configx.ACLPolicyFile)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "trces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewlogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	cfg := Config{
		CommitLog:  commitLog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(&cfg)
	}

	// 1.2 setup listen
	// port :0 means auto assign a free port
	listen, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// 1.3 setup new grpc server
	serverTLSConfig, err := configx.SetupTLSConfig(configx.TLSConfig{
		CAFile:        configx.CAFile,
		CertFile:      configx.ServerCertFile,
		KeyFile:       configx.ServerKeyFile,
		ServerAddress: listen.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	server, err := NewGRPCServer(&cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// 1.4 run blocking serv in go routine
	// Note that in gRPC-Go, RPCs operate in a blocking/synchronous mode,
	// which means that the RPC call waits for the server to respond,
	//and will either return a response or an error.
	go func() {
		// Serve is a blocking call, has to run in go routine
		// otherwise any code below it wouldn't able to run.
		server.Serve(listen)
	}()

	// 2. setup client stub

	// 2.1 setup channel
	/*
		clientTLSConfig, err := configx.SetupTLSConfig(configx.TLSConfig{
			CAFile:   configx.CAFile,
			CertFile: configx.ClientCertFile,
			KeyFile:  configx.ClientKeyFile,
		})
		require.NoError(t, err)

		// use our CA as the client's root CA,will used to verify the server.
		clientCreds := credentials.NewTLS(clientTLSConfig)
		//clientOptions := []grpc.DialOption{grpc.WithInsecure()}
		clientConnection, err := grpc.Dial(listen.Addr().String(), grpc.WithTransportCredentials(clientCreds))
		require.NoError(t, err)
		// 2.2 setup client stub intance
		client = api.NewLogClient(clientConnection)
	*/

	// 2.3 set up new client
	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := configx.SetupTLSConfig(configx.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   configx.CAFile,
			Server:   false,
		})
		require.NoError(t, err)

		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(listen.Addr().String(), opts...)
		require.NoError(t, err)

		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(configx.RootClientCertFile, configx.RootClientKeyFile)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(configx.NobodyClientCertFile, configx.NobodyClientKeyFile)

	// 3. return instances
	return rootClient, nobodyClient, &cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		listen.Close()
		//commitLog.Remove()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

func testUnauthorized(
	t *testing.T,
	_,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	// check produce permission
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}

	// check consume permission
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}

	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}

}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	want := api.Record{
		Value: []byte("hello world"),
	}

	produceRequest := api.ProduceRequest{
		Record: &want,
	}

	//We also pass a context.Context object which lets us change our RPC’s behavior if necessary,
	// such as time-out/cancel an RPC in flight.
	produceResponse, err := client.Produce(ctx, &produceRequest)
	require.NoError(t, err)

	consumeRequest := api.ConsumeRequest{
		Offset: produceResponse.Offset,
	}
	consumeResponse, err := client.Consume(ctx, &consumeRequest)
	require.NoError(t, err)
	require.Equal(t, want.Value, consumeResponse.Record.Value)
	require.Equal(t, want.Offset, consumeResponse.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	record := api.Record{
		Value: []byte("hello world"),
	}
	prequest := api.ProduceRequest{
		Record: &record,
	}
	produce, err := client.Produce(ctx, &prequest)
	require.NoError(t, err)

	crequest := api.ConsumeRequest{
		Offset: produce.Offset + 1,
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

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	// code block

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err := stream.Send(&api.ProduceRequest{
				Record: record,
			})

			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
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
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}
