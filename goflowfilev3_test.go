package goflowfilev3

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestReadFile(t *testing.T) {
	file, err := os.Open("testdata/dcfa9c64-d0c3-443d-a9b7-2fbb8720ddda")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	unpackager := NewFlowFileUnpackagerV3()

	attributes, err := unpackager.UnpackageFlowFile(file)
	if err != nil {
		t.Fatalf("Error unpackaging flow file:%s", err)
	}
	if len(attributes) != 3 {
		t.Fatalf("Expected 3 attributes, got %d", len(attributes))
	}
	if attributes["filename"] != "03391067-5e15-48b2-9962-caee2b422168" {
		t.Fatalf("Expected filename to be 03391067-5e15-48b2-9962-caee2b422168, got %s", attributes["filename"])
	}
	if attributes["path"] != "./" {
		t.Fatalf("Expected path to be ./, got %s", attributes["path"])
	}
	if attributes["uuid"] != "03391067-5e15-48b2-9962-caee2b422168" {
		t.Fatalf("Expected uuid to be 03391067-5e15-48b2-9962-caee2b422168, got %s", attributes["uuid"])
	}
	var data bytes.Buffer
	err = unpackager.GetData(file, &data)
	if err != nil {
		t.Fatalf("Error getting payload:%s", err)
	}
	if data.String() != "Re(3a@x<KX" {
		t.Fatalf("Expected payload to be Re(3a@x<KX, got %s", data.String())
	}

	attributes, err = unpackager.UnpackageFlowFile(file)
	if err != nil {
		t.Fatalf("Error unpackaging flow file:%s", err)
	}
	if len(attributes) != 3 {
		t.Fatalf("Expected 3 attributes, got %d", len(attributes))
	}
	if attributes["filename"] != "00b82f54-ba92-43df-941b-477773006eb5" {
		t.Fatalf("Expected filename to be 00b82f54-ba92-43df-941b-477773006eb5, got %s", attributes["filename"])
	}
	if attributes["path"] != "./" {
		t.Fatalf("Expected path to be ./, got %s", attributes["path"])
	}
	if attributes["uuid"] != "00b82f54-ba92-43df-941b-477773006eb5" {
		t.Fatalf("Expected uuid to be 03391067-5e15-48b2-9962-caee2b422168, got %s", attributes["uuid"])
	}
	var data2 bytes.Buffer
	err = unpackager.GetData(file, &data2)
	if err != nil {
		t.Fatalf("Error getting payload:%s", err)
	}
	if data2.String() != "Cq(/)W/wgy" {
		t.Fatalf("Expected payload to be Cq(/)W/wgy, got %s", data2.String())
	}
	_, err = unpackager.UnpackageFlowFile(file)
	if err != io.EOF {
		t.Fatalf("Expected EOF, got %s", err)
	}

}

func Test_PackageFlowFile(t *testing.T) {
	packager := NewFlowFilePackagerV3()
	var testData = []byte("test data")

	// warning: the order of the attributes is not guaranteed - the map is unordered
	testAttributes := map[string]string{
		"key1": "value1",
	}

	testCases := []struct {
		name         string
		inputData    []byte
		attributes   map[string]string
		expectedData []byte
	}{
		{
			name:       "Empty Data and Attributes",
			inputData:  testData,
			attributes: nil,
		},
		{
			name:       "Non-Empty Data and Attributes",
			inputData:  testData,
			attributes: testAttributes,
		},
	}

	out := &bytes.Buffer{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			in := bytes.NewReader(tc.inputData)

			err := packager.PackageFlowFile(in, out, tc.attributes, int64(len(tc.inputData)))
			if err != nil {
				t.Errorf("PackageFlowFile() error = %v", err)
			}

		})
	}

	result := out.Bytes()
	if !bytes.HasPrefix(result, []byte("NiFiFF3")) {
		t.Errorf("PackageFlowFile() did not write the correct magic header")
	}

	hexResult := hex.EncodeToString(result)
	expectedResult := "4e694669464633000000000000000000097465737420646174614e694669464633000100046b657931000676616c7565310000000000000009746573742064617461"
	if hexResult != expectedResult {
		t.Errorf("PackageFlowFile() did not write the correct output %v", hexResult)
	}
}

func TestReader(t *testing.T) {
	file, err := os.Open("testdata/dcfa9c64-d0c3-443d-a9b7-2fbb8720ddda")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	unpackager := NewFlowFileUnpackagerV3()

	//################## first flowfile in file
	attributes, err := unpackager.UnpackageFlowFile(file)
	if err != nil {
		t.Fatalf("Error unpackaging flow file:%s", err)
	}
	if len(attributes) != 3 {
		t.Fatalf("Expected 3 attributes, got %d", len(attributes))
	}

	reader, datasize, err := unpackager.GetDataReader(file)
	if err != nil {
		t.Fatalf("Error getting payload:%s", err)
	}
	// read the data from reader
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Error reading data from reader:%s", err)
	}
	if datasize != 10 {
		t.Fatalf("Expected data size to be 10, got %d", datasize)
	}

	result := bytes.Compare(data, []byte("Re(3a@x<KX"))
	if result != 0 {
		t.Fatalf("Expected payload to be Re(3a@x<KX, got %s", data)
	}

	//################## second flowfile in file
	attributes2, err := unpackager.UnpackageFlowFile(file)
	if err != nil {
		t.Fatalf("Error unpackaging flow file:%s", err)
	}
	if len(attributes2) != 3 {
		t.Fatalf("Expected 3 attributes, got %d", len(attributes2))
	}
	reader2, datasize2, err := unpackager.GetDataReader(file)
	if err != nil {
		t.Fatalf("Error getting payload:%s", err)
	}
	if datasize2 != 10 {
		t.Fatalf("Expected data size to be 10, got %d", datasize2)
	}

	// read the data from reader
	data2, err := io.ReadAll(reader2)
	if err != nil {
		t.Fatalf("Error reading data from reader:%s", err)
	}

	result2 := bytes.Compare(data2, []byte("Cq(/)W/wgy"))
	if result2 != 0 {
		t.Fatalf("Expected payload to be Cq(/)W/wgy, got %s", data2)
	}
}
