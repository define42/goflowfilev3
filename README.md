# goflowfilev3
Golang library for reading and writing Apache NiFi FlowFile v3


## Example code
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

