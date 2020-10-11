package main

import (
	"fmt"
	"rain/rs"
)

func main() {
	encoder, _ := rs.New(6, 0b100011011)
	fmt.Println(encoder.Multiply(168, 44))
}
