package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth = 4
	posWidth = 8
	entWidth = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / uint64(entWidth)) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * uint64(entWidth)

	if i.size < pos+uint64(entWidth) {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+uint64(offWidth)])
	pos = enc.Uint64(i.mmap[pos+uint64(offWidth) : pos+uint64(entWidth)])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	//validate to have space to write the entry
	if uint64(len(i.mmap)) < i.size+uint64(entWidth) {
		return io.EOF
	}
	//encode the offset
	enc.PutUint32(i.mmap[i.size:i.size+uint64(offWidth)], off)
	//encode the pos
	enc.PutUint64(i.mmap[i.size+uint64(offWidth):i.size+uint64(entWidth)], pos)
	//increment for the next write
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
