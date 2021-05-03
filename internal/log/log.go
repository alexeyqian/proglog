package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/alexeyqian/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex
	
	Dir string
	Config Config

	activeSegment *segment
	segments []*segment
}

func NewLog(dir string, c Config)(*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		C.Segment.MaxIndexBytes = 1024
	}

	log := Log{
		Dir: dir,
		Config: c,
	}

	return &log, log.setup()
}

func (log *Log) setup() error {
	files, err := ioutil.ReadDir(log.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64 
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		off, _ := strconv.ParseUnit(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i:=0; i < len(baseOffsets); i++{
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains dup for index and store, we skip the dup
		i++
	}

	if log.segments == nil {
		if err = log.newSegment(log.Config.Segment.InitilOffset); err != nil {
			return err
		}
	}

	return nil
}
/*
 * We use a RWMutex to grant access to reads when there isn’t a write holding the lock. 
 * If you felt so inclined, you could optimize this further and make the locks per segment 
 * rather than across the whole log. 
 * (I haven’t done that here because I want to keep this code simple.)
*/
func (log *Log) Append(record *api.Record)(uint64, error){
	log.mu.Lock()
	defer log.mu.Unlock()

	off, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if log.activeSegment.IsMaxed(){
		err = log.newSegment(off + 1)
	}

	return off, err
}

func (log *Log)Read(off uint64)(*api.Record, error){
	log.mu.RLock()
	defer log.mu.RUnlock()

	var s *segment
	for _, segment := range log.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)
}

func (log *Log) Close() error{
	log.mu.Lock()
	defer log.mu.Unlock()

	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}

	return os.RemoveAll(log.Dir)
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}

	return log.setup()
}

func (log *Log) LowestOffset()(uint64, error){
	log.mu.RLock()
	defer log.mu.RUnlock()

	return log.segments[0].baseOffset, nil
}

func (log *Log)HighestOffset()(uint64, error){
	log.mu.RLock()
	defer log.mu.RUnlock()

	off := log.segments[len(log.segments) - 1].nextOffset
	if off == 0{
		return 0, nil
	}

	return off - 1, nil
}

// removes all segments whose highese offset is lower than parameter:lowest
func (log *Log) Truncate(lowest uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	var segments []*segment
	for _, s := range log.segments{
		if s.nextOffset <= lowest + 1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}

	log.segments = segments
	return nil
}

type originReader struct {
	*store,
	off int64
}

// returns an io.Reader to read the whole log.
func (log *Log) Reader() io.Reader {
	log.mu.RLock()
	defer log.mu.RUnlock()

	readers := make([]io.Reader, len(log.segments))
	for i, segment := range log.segments{
		readers[i] = &originReader{segment.store, 0}
	}

	return io.MultiReader(readers...)
}

func (o *originReader) Read(p []byte)(int, error){
	n, err := o.ReadAt(p, o.ff)
	o.off += int64(n)
	return n, err
}

func (log *Log) newSegment(off uint64) error {
	s, err := newSegment(log.Dir, off, log.Config)
	if err != nil {
		return err
	}

	log.segments = append(log.segments, s)
	log.activeSegment = s
	return nil
}



