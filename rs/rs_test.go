package rs

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestFiniteFieldMultiply(t *testing.T) {
	t.Log("Running test TestFiniteFieldMultiply")
	file, err := os.Open("test.in")
	if err != nil {
		t.Error("Open test file failed")
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	encoder, err := New(6, 0b100011011)
	if err != nil {
		t.Error("Create encoder failed")
		return
	}
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error("Read line from test file failed")
			return
		}
		values := strings.Split(string(line), " ")
		x, err := strconv.Atoi(values[0])
		if err != nil {
			t.Error("Test file format invalid")
			return
		}
		y, err := strconv.Atoi(values[1])
		if err != nil {
			t.Error("Test file format invalid")
			return
		}
		z, err := strconv.Atoi(values[2])
		if err != nil {
			t.Error("Test file format invalid")
			return
		}
		if encoder.Multiply(byte(x), byte(y)) != byte(z) {
			t.Error("Failed! Result different from pyfinite")
		}
	}
}
