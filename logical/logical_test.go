package logical

import (
	"bytes"
	"path"
	"testing"
)

func TestLogicalBasicNewFile(t *testing.T) {
	tmp := t.TempDir()

	log, err := NewLogical(path.Join(tmp, "00001.logical"))
	if err != nil {
		t.Errorf("failed to create new logical: %s", err)
	}
	defer log.Close()

	data1 := []byte("hello world this is a image or some other file lol")

	offset1, err := log.WriteBytes(data1)
	if err != nil {
		t.Errorf("failed to write bytes: %s", err)
	}

	data2 := []byte("this is some other data")
	offset2, err := log.WriteBytes(data2)
	if err != nil {
		t.Errorf("failed to write bytes: %s", err)
	}

	data2Got, err := log.ReadBytes(offset2, int64(len(data2)))
	if err != nil {
		t.Errorf("failed to read bytes")
	}

	if !bytes.Equal(data2, data2Got) {
		t.Errorf("data doesn't match")
	}

	data1Got, err := log.ReadBytes(offset1, int64(len(data1)))
	if err != nil {
		t.Errorf("failed to read bytes")
	}

	if !bytes.Equal(data1, data1Got) {
		t.Errorf("data doesn't match")
	}
}

func TestLogicalBasicExistingFile(t *testing.T) {
	tmp := t.TempDir()
	p := path.Join(tmp, "00001.logical")

	log, err := NewLogical(p)
	if err != nil {
		t.Errorf("failed to create new logical: %s", err)
	}

	data1 := []byte("hello world this is a image or some other file lol")

	offset1, err := log.WriteBytes(data1)
	if err != nil {
		t.Errorf("failed to write bytes: %s", err)
	}

	data2 := []byte("this is some other data")
	offset2, err := log.WriteBytes(data2)
	if err != nil {
		t.Errorf("failed to write bytes: %s", err)
	}

	data2Got, err := log.ReadBytes(offset2, int64(len(data2)))
	if err != nil {
		t.Errorf("failed to read bytes")
	}

	if !bytes.Equal(data2, data2Got) {
		t.Errorf("data doesn't match")
	}

	data1Got, err := log.ReadBytes(offset1, int64(len(data1)))
	if err != nil {
		t.Errorf("failed to read bytes")
	}

	if !bytes.Equal(data1, data1Got) {
		t.Errorf("data doesn't match")
	}

	log.Close()

	log2, err := NewLogical(p)
	if err != nil {
		t.Errorf("failed to create logical: %s", err)
	}
	defer log2.Close()

	data2Got, err = log2.ReadBytes(offset2, int64(len(data2)))
	if err != nil {
		t.Errorf("failed to read bytes")
	}

	if !bytes.Equal(data2, data2Got) {
		t.Errorf("data doesn't match")
	}

	data1Got, err = log2.ReadBytes(offset1, int64(len(data1)))
	if err != nil {
		t.Errorf("failed to read bytes")
	}

	if !bytes.Equal(data1, data1Got) {
		t.Errorf("data doesn't match")
	}
}
