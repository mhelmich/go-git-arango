package main

import (
	"fmt"
	"os"
)

func main() {
	_, err := OpenRepo("arangit")
	if err != nil {
		fmt.Printf("ERROR: %s", err.Error())
		os.Exit(-1)
	}
}
