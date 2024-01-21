# goflowfilev3
Golang library for reading and writing Apache NiFi FlowFile v3


## Reading NiFi flowfile v3 with goflowfilev3
```
package main

import (
        "fmt"
        "os"
        "github.com/define42/goflowfilev3"
)

func main() {
        file, err := os.Open("testdata/dcfa9c64-d0c3-443d-a9b7-2fbb8720ddda") // Replace with your file path
        if err != nil {
                fmt.Println("Error opening file:", err)
                return
        }
        defer file.Close()

        unpackager := goflowfilev3.NewFlowFileUnpackagerV3()

        for {
                attributes, err := unpackager.UnpackageFlowFile(file)
                if err != nil {
                        fmt.Println("Error unpackaging flow file:", err)
                        return
                }

                for key, value := range attributes {
                        fmt.Println("Key:", key, "Value:", value)
                }
                err = unpackager.GetData(file, os.Stdout)

                if err != nil {
                        fmt.Println("Error getting payload:", err)
                        return
                }

        }
}

```

## Writing NiFi flowfile v3 with goflowfilev3
```
package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/define42/goflowfilev3"
)

func main() {
	packager := goflowfilev3.NewFlowFilePackagerV3()
	fileContent := "Hello, NiFi!"
	attributes := map[string]string{
		"Author": "John Doe",
		"Type":   "Example",
	}

	file, err := os.Create("flowfile.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	err = packager.PackageFlowFile(bytes.NewReader([]byte(fileContent)), file, attributes, int64(len(fileContent)))
	if err != nil {
		fmt.Println("Error creating flow file:", err)
		return
	}
}

```