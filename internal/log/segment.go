package log

// segment wraps the index and store types to coordinate operations across the two.
// For example, when the log appends a record to the active segment, 
// the segment needs to write the data to its store and add a new entry in the index.
// Similarily for reads, the segment needs to look up the the entry from the index
// and then fetch the data from the store.

import (
	"fmt"
	"os"
	"path"

	api "github.com/alexeyqian/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store *store
	index *index
	baseOffset, nextOffset uint64
	config Config
}

func newSegment(dir string, baseOffset uint64, c Config)(*segment, error){
	s := &segment{
		baseOffset: baseOffset,
		config: c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpendFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	if off,_,err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset // when index is empty/new created.
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record)(offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err = s.index.Write(
		// index offsets are relative to base offset
		// baseOffset and nextOffset are both absolute offsets
		uint32(s.nextOffset - uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}