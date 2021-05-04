package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth // entity width
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	// grow the file to the max index size before memory mapping
	// since once file is memory mapped, we can't resize them,
	// so we have to set it to max it now.
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return &idx, nil
}

// indexOff is offset of the index
// off is the offset of the store, pos is the postion of the store
// we use relative offsets to reduce the size of the indexes by storing offsets as uint32s.
// since it might be millions of records every day which save a lot of space in long run.
func (i *index) Read(indexOff int64) (off uint32, indexPos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	realIndexOff := uint32(indexOff)
	if indexOff == -1 {
		realIndexOff = uint32((i.size / entWidth) - 1)
	}

	indexPos = uint64(realIndexOff) * entWidth
	if i.size < indexPos+entWidth {
		return 0, 0, io.EOF
	}

	dataOff := enc.Uint32(i.mmap[indexPos : indexPos+offWidth])
	dataPos := enc.Uint64(i.mmap[indexPos+offWidth : indexPos+entWidth])
	return dataOff, dataPos, nil
}

// write offset and position of store into index
// offset of index and store are identical.
// index entity: |offset 32 bits|position 64 bits|
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Close() error {
	// sync memory mapped data to file content
	// TODO: do we need to close the mmap?
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// sync persisted file content to storage
	if err := i.file.Sync(); err != nil {
		return err
	}

	// truncating the index files to remove the empty space
	// which we put it when open/new index
	// this will make the last entry at the end of file.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}
