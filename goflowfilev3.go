package goflowfilev3

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unicode/utf8"
)

const (
	magicHeaderLength = 7
	magicHeader       = "NiFiFF3"
)

type FlowFileUnpackagerV3 struct {
	nextHeader        []byte
	haveReadSomething bool
	expectedNumBytes  int64
}

func NewFlowFileUnpackagerV3() *FlowFileUnpackagerV3 {
	return &FlowFileUnpackagerV3{}
}

func (ffu *FlowFileUnpackagerV3) ReadFieldLength(in io.Reader) (int, error) {
	var buffer [4]byte
	_, err := io.ReadFull(in, buffer[:2])
	if err != nil {
		return 0, err
	}
	if buffer[0] == 0xff && buffer[1] == 0xff {
		_, err = io.ReadFull(in, buffer[2:])
		if err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(buffer[:])), nil
	} else {
		return int(buffer[0])<<8 + int(buffer[1]), nil
	}
}

func (ffu *FlowFileUnpackagerV3) ReadString(in io.Reader) (string, error) {
	length, err := ffu.ReadFieldLength(in)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", errors.New("string length cannot be zero")
	}
	bytes := make([]byte, length)
	err = ffu.FillBuffer(in, bytes, length)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(bytes) {
		return "", errors.New("invalid UTF-8 bytes")
	}
	return string(bytes), nil
}

func (ffu *FlowFileUnpackagerV3) FillBuffer(in io.Reader, buffer []byte, length int) error {
	bytesRead, err := io.ReadFull(in, buffer[:length])
	if err != nil {
		return err
	}
	if bytesRead != length {
		return errors.New("not enough bytes read")
	}
	return nil
}

func (ffu *FlowFileUnpackagerV3) ReadAttributes(in io.Reader) (map[string]string, error) {
	numAttributes, err := ffu.ReadFieldLength(in)
	if err != nil {
		return nil, err
	}
	if numAttributes == 0 {
		return nil, errors.New("flow files cannot have zero attributes")
	}

	attributes := make(map[string]string)
	for i := 0; i < numAttributes; i++ {
		key, err := ffu.ReadString(in)
		if err != nil {
			return nil, err
		}
		value, err := ffu.ReadString(in)
		if err != nil {
			return nil, err
		}
		attributes[key] = value
	}
	return attributes, nil
}

func (ffu *FlowFileUnpackagerV3) ReadLong(in io.Reader) (int64, error) {
	var readBuffer = make([]byte, 8)
	err := ffu.FillBuffer(in, readBuffer, 8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(readBuffer)), nil
}

func (ffu *FlowFileUnpackagerV3) ReadHeader(in io.Reader) ([]byte, error) {
	header := make([]byte, magicHeaderLength)
	_, err := io.ReadFull(in, header)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errors.New("not in FlowFile-v3 format")
	}
	return header, nil
}

func (ffu *FlowFileUnpackagerV3) GetData(in io.Reader, out io.Writer) error {
	n, err := io.CopyN(out, in, ffu.expectedNumBytes)
	if n != ffu.expectedNumBytes {
		return errors.New("not enough bytes read")
	}
	ffu.expectedNumBytes = 0
	if err != nil {
		return err
	}
	return nil
}

func (ffu *FlowFileUnpackagerV3) UnpackageFlowFile(in io.Reader) (map[string]string, error) {
	if !ffu.haveReadSomething {
		header, err := ffu.ReadHeader(in)
		if err != nil {
			return nil, err
		}
		ffu.nextHeader = header
	}

	if !bytes.Equal(ffu.nextHeader, []byte(magicHeader)) {
		return nil, errors.New("not in FlowFile-v3 format")
	}

	attributes, err := ffu.ReadAttributes(in)
	if err != nil {
		return nil, err
	}

	expectedNumBytes, err := ffu.ReadLong(in)
	if err != nil {
		return nil, err
	}
	ffu.expectedNumBytes = expectedNumBytes

	return attributes, nil
}
