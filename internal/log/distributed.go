package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github.com/alexeyqian/proglog/api/v1"
)

var (
	_ raft.FSM = (*fsm)(nil)
)

type fsm struct {
	log *Log
}

type DistributedLog struct {
	config Config
	log *Log
	raft *raft.Raft
}

func NewDistributedLog(dataDir string, config Config)(*DistributedLog, error){
	l := DistributedLog{
		config: config,
	}

	if err := l.setupLog(dataDir); err != nil{
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return &l, nil
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	// finite state machine
	fsm := &fsm{log: l.log} 
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// stable store is a key-value store where Raft stores important metadata.
	// like server's current term or the candidate the server voted for.
	// Bolt is an embeded and persisted key-value database for Go
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.join(dataDir, "raft", "stable"),
	)

	if err != nil {
		return err
	}

	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		o.Stderr,
	)

	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	// localID is the unique ID for this server
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID

	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}

	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}

	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}

	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	// create raft instance and bootstrap the cluster
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)

	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil{
		return err
	}

	if l.config.Raft.Bootstrap && !hasState{
		config := raft.Configuration{
			Servers: []raft.Server{{ID: config.LocalID, Address: transport.LocalAddr()}}
		}

		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}

// raft replicate the command to majority of the raft servers
// and ultimately append the record to a majority of Raft servers.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error){
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil{
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil{
		return nil, future.Error()
	}

	res := future.Response()
	// check if the res is an error
	if err, ok := res.(error): ok{
		return nil, err
	}

	return res, nil
}

func (l *DistributedLog) Read(offset uint64)(*api.Record, error){
	// using relaxed consistency instead of strong consistency here.
	return l.log.Read(offset)
}

type RequestType uint8

const (
	AppendRequestType RequestType  = 0
)

func (l *fsm)Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType{
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

func (l *fsm) applyAppend(b []byte) interface{}{
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil{
		return err
	}

	offset, err := l.log.Append(req.Reocrd)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{Offset: offset}
}

// raft calls Snapshot according to configured Snapshortinterval
// and SnapshotThreshold (how many logs since the the last snapshot before making a new snapshot)
func (f *fsm) Snapshot() (raft.FSMSnapshot, error){
	r :=f.log.Reader()
	// return a reader that read all log's data
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil{
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release(){}

// restore an fsm from a snapshot
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++{
		_, err := io.ReadFull(r, b)
		if err == io.EOF{
			break
		}else if err != nil{
			return err
		}

		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil{
			return err
		}

		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil{
			return err
		}

		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil{
				return err
			}
		}

		if _, err = f.log.Append(record); err != nil{
			return err
		}

		buf.Reset()
	}

	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error){
	log, err := NewLog(dir, c)
	if err != nil{
		return nil, err
	}
	return &logStore{log}, nil
}


func (l *logStore) FirstIndex() (uint64, error){
	return l.LowestOffset()
}

func (l *logStore) LastIndex()(uint64, error){
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil{
		return err
	}

	out.Data = in.Value
	out.index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term

	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error{
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error{
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data
			Term: record.Term
			Type: uint32(record.Type)
		}); err != nil{
			return err
		}
	}

	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Trancate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct{
	ln net.Listener
	// accept incomming connections
	serverTLSConfig *tls.Config
	// create outgoing connections
	peerTLSConfig *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer{
	return &StreamLayer{
		ln: ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig: peerTLSConfig,
	}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error){
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// identify to mux this is a raft rpc
	// to multiplexing raft on the same port
	// as our Log gRPC requests.
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil{
		return nil, err
	}

	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err
}

func (s *StreamLayer) Accept() (net.Conn, error){
	
}
