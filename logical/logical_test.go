package logical

import (
	"bytes"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogicalBasicNewFile(t *testing.T) {
	tmp := t.TempDir()

	log, err := NewLogical(path.Join(tmp, "00001.logical"))
	assert.NoError(t, err)
	defer log.Close()

	data1 := []byte("hello world this is a image or some other file lol")

	offset1, err := log.WriteBytes(data1)
	assert.NoError(t, err)

	data2 := []byte("this is some other data")
	offset2, err := log.WriteBytes(data2)
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

	offset1, err := log.WriteBytes(data1)
	assert.NoError(t, err)

	data2 := []byte("this is some other data")
	offset2, err := log.WriteBytes(data2)
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
