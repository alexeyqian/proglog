package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

const (
	lenWidth = 8 // number of bytes used to store the record's length
)


var (
	enc = binary.BigEndian
)

type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File)(*store, error) {
	fi, err := os.State(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	}, nil
}

// return bytes written at pos, beginning of pos stores size data, then real data
func (s *store) Append(p []byte) (n uint64, pos uint64, err error){
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	if err := binary.Write(s.buf, enc, unit64(len(p))); err != nil {
		return 0, 0, error
	}

	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += unit64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Unit64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64)(int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush; err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error{
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}