package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "github.com/alexeyqian/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type fn func(*testing.T, *Log)

func TestLog(t *testing.T) {
	funcs := make(map[string]fn)

	funcs["a"] = testAppendRead
	funcs["b"] = testOutOfRangeErr
	funcs["c"] = testInitExisting
	funcs["d"] = testReader
	funcs["e"] = testTrancate

	for scenario, fn := range funcs {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	rec := api.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(&rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	readRec, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, rec.Value, readRec.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	//require.Error(t, err)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	rec := api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(&rec)
		require.NoError(t, err)
	}

	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// creating new log instance by opening existing log and check it again
	_, err = NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	rec := api.Record{
		Value: []byte("HELLO WORLD"),
	}

	off, err := log.Append(&rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	rec2 := api.Record{}
	err = proto.Unmarshal(b[lenWidth:], &rec2)
	require.NoError(t, err)
	require.Equal(t, rec.Value, rec2.Value)
}

func testTrancate(t *testing.T, log *Log) {
	rec := api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(&rec)
		require.NoError(t, err)
	}

	// remove old segments below 1
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)

}
