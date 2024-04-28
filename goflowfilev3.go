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
	maxValue2Bytes    = 65535
)

type limitedReader struct {
	R io.Reader // underlying reader
	N int64     // max bytes remaining
}

func (l *limitedReader) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, io.EOF // prevent reading beyond the limit
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N] // limit p to N bytes
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	return
}

func NewFlowFilePackagerV3() *FlowFilePackagerV3 {
	return &FlowFilePackagerV3{}
}

// FlowFilePackagerV3 implements the packaging of NiFi FlowFile V3
type FlowFilePackagerV3 struct {
	writeBuffer [8]byte
}

// PackageFlowFile packages the given flow file data and attributes to the output stream
func (p *FlowFilePackagerV3) PackageFlowFile(in io.Reader, out io.Writer, attributes map[string]string, fileSize int64) error {
	_, err := out.Write([]byte(magicHeader))
	if err != nil {
		return err
	}

	if attributes == nil {
		err = p.writeFieldLength(out, 0)
		if err != nil {
			return err
		}
	} else {
		err = p.writeFieldLength(out, len(attributes))
		if err != nil {
			return err
		}
		for key, value := range attributes {
			err = p.writeString(out, key)
			if err != nil {
				return err
			}
			err = p.writeString(out, value)
			if err != nil {
				return err
			}
		}
	}

	err = p.writeLong(out, fileSize)
	if err != nil {
		return err
	}

	_, err = io.Copy(out, in)
	return err
}

func (p *FlowFilePackagerV3) writeString(out io.Writer, val string) error {
	bytes := []byte(val)
	err := p.writeFieldLength(out, len(bytes))
	if err != nil {
		return err
	}
	_, err = out.Write(bytes)
	return err
}

func (p *FlowFilePackagerV3) writeFieldLength(out io.Writer, numBytes int) error {
	if numBytes < maxValue2Bytes {
		p.writeBuffer[0] = byte(numBytes >> 8)
		p.writeBuffer[1] = byte(numBytes)
		_, err := out.Write(p.writeBuffer[:2])
		return err
	}
	p.writeBuffer[0] = 0xff
	p.writeBuffer[1] = 0xff
	binary.BigEndian.PutUint32(p.writeBuffer[2:], uint32(numBytes))
	_, err := out.Write(p.writeBuffer[:6])
	return err
}

func (p *FlowFilePackagerV3) writeLong(out io.Writer, val int64) error {
	binary.BigEndian.PutUint64(p.writeBuffer[:], uint64(val))
	_, err := out.Write(p.writeBuffer[:])
	return err
}

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

// GetDataReader returns an io.Reader that will read only the expected number of bytes from input
func (ffu *FlowFileUnpackagerV3) GetDataReader(in io.Reader) (io.Reader, int64, error) {
	return &limitedReader{R: in, N: ffu.expectedNumBytes}, ffu.expectedNumBytes, nil
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
