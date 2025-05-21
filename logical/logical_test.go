package logical

import (
	"bytes"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestLogicalBasicNewFile(t *testing.T) {
	tmp := t.TempDir()

	log, err := NewLogical(path.Join(tmp, "00001.logical"))
	assert.NoError(t, err)
	defer log.Close()

	data1 := []byte("hello world this is a image or some other file lol")
	uuid1 := uuid.NewString()

	offset1, err := log.WriteFile(uuid1, data1)
	assert.NoError(t, err)

	data2 := []byte("this is some other data")

	uuid2 := uuid.NewString()
	offset2, err := log.WriteFile(uuid2, data2)
	assert.NoError(t, err)

	data2Got, err := log.ReadBytes(offset2, int64(len(data2)))
	assert.NoError(t, err)

	if !bytes.Equal(data2, data2Got) {
		t.Errorf("data doesn't match")
	}

	data1Got, err := log.ReadBytes(offset1, int64(len(data1)))
	assert.NoError(t, err)

	if !bytes.Equal(data1, data1Got) {
		t.Errorf("data doesn't match")
	}
}

func TestLogicalBasicExistingFile(t *testing.T) {
	tmp := t.TempDir()
	p := path.Join(tmp, "00001.logical")

	log, err := NewLogical(p)
	assert.NoError(t, err)

	data1 := []byte("hello world this is a image or some other file lol")
	uuid1 := uuid.NewString()

	offset1, err := log.WriteFile(uuid1, data1)
	assert.NoError(t, err)

	data2 := []byte("this is some other data")

	uuid2 := uuid.NewString()
	offset2, err := log.WriteFile(uuid2, data2)
	assert.NoError(t, err)

	data2Got, err := log.ReadBytes(offset2, int64(len(data2)))
	assert.NoError(t, err)

	if !bytes.Equal(data2, data2Got) {
		t.Errorf("data doesn't match")
	}

	data1Got, err := log.ReadBytes(offset1, int64(len(data1)))
	assert.NoError(t, err)

	if !bytes.Equal(data1, data1Got) {
		t.Errorf("data doesn't match")
	}

	log.Close()

	log2, err := NewLogical(p)
	assert.NoError(t, err)
	defer log2.Close()

	data2Got, err = log2.ReadBytes(offset2, int64(len(data2)))
	assert.NoError(t, err)

	if !bytes.Equal(data2, data2Got) {
		t.Errorf("data doesn't match")
	}

	data1Got, err = log2.ReadBytes(offset1, int64(len(data1)))
	assert.NoError(t, err)

	if !bytes.Equal(data1, data1Got) {
		t.Errorf("data doesn't match")
	}
}

func TestRecover(t *testing.T) {
	tmp := t.TempDir()
	p := path.Join(tmp, "00001.logical")

	log, err := NewLogical(p)
	assert.NoError(t, err)

	data1 := []byte("hello world this is a image or some other file lol")
	uuid1 := uuid.NewString()

	offset1, err := log.WriteFile(uuid1, data1)
	assert.NoError(t, err)

	data2 := []byte("this is some other data")

	uuid2 := uuid.NewString()
	offset2, err := log.WriteFile(uuid2, data2)
	assert.NoError(t, err)

	log.Close()

	log2, err := NewLogical(p)
	assert.NoError(t, err)
	defer log2.Close()

	metadata, err := log2.Recover()
	assert.NoError(t, err)

	m1 := metadata[uuid1]
	assert.Equal(t, offset1, m1.offset)
	assert.Equal(t, int64(len(data1)), m1.size)

	m2 := metadata[uuid2]
	assert.Equal(t, offset2, m2.offset)
	assert.Equal(t, int64(len(data2)), m2.size)
}
