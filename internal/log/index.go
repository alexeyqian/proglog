package log

import (
	"io"
	"os"
	"github.com/tysontate/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth = offWidth + posWidth // entity width
)

type index struct {
	file *os.File
	mmap gommap.Mmap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.State(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Szie())
	// grow the file to the max index size before memory mapping 
	// since once file is memory mapped, we can't resize them,
	// so we have to set it to max it now.
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); 
		err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// in is offset of the index
// off is the offset of the store, pos is the postion of the store
// we use relative offsets to reduce the size of the indexes by storing offsets as uint32s. 
// since it might be millions of records every day which save a lot of space in long run.
func (i *index) Read(inOff int64)(off uint32, pos uint64, err error){
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if inOff == -1 {
		off = uint32((i.size / entWidth) - 1)
	} else {
		off = uint32(inOff)
	}

	pos = uint64(off) * entWidth
	if i.size < pos + entWidth {
		return 0, 0, io.EOF
	}

	off = enc.Uint32(i.mmap[pos:pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth:pos+entWidth])
	return off, pos, nil
}

// write offset and position of store into index
// offset of index and store are identical.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth{
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	retur nil
}

func (i *index)Close() error{
	// sync data to persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return error
	}

	// sync persisted file content to storage
	if err := i.file.Sync(); err != nil {
		return err
	}

	// truncating the index files to remove the empty space which we put it when open/new index
	// and put the last entry at the end of file.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Name() string{
	return i.file.Name()
}