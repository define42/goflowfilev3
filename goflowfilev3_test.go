package goflowfilev3

import (
	"bytes"
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
