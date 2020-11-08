package rs

import (
	"math/rand"
	"rain/util"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type MultiplyTestCast struct {
	x, y, xy byte
}

var (
	dataShard = 4
	cases     = []MultiplyTestCast{
		{x: 103, y: 226, xy: 121},
		{x: 116, y: 158, xy: 167},
		{x: 242, y: 188, xy: 91},
		{x: 6, y: 173, xy: 195},
		{x: 40, y: 70, xy: 30},
		{x: 198, y: 233, xy: 152},
		{x: 124, y: 139, xy: 45},
		{x: 204, y: 225, xy: 98},
		{x: 52, y: 12, xy: 107},
		{x: 12, y: 7, xy: 36},
		{x: 100, y: 81, xy: 22},
		{x: 82, y: 102, xy: 30},
		{x: 37, y: 140, xy: 186},
		{x: 218, y: 255, xy: 239},
		{x: 202, y: 135, xy: 224},
		{x: 5, y: 102, xy: 229},
		{x: 179, y: 139, xy: 83},
		{x: 78, y: 127, xy: 62},
		{x: 3, y: 165, xy: 244},
		{x: 194, y: 24, xy: 222},
		{x: 13, y: 156, xy: 86},
		{x: 43, y: 149, xy: 93},
		{x: 71, y: 209, xy: 203},
		{x: 24, y: 112, xy: 236},
		{x: 35, y: 248, xy: 33},
		{x: 99, y: 178, xy: 5},
		{x: 100, y: 220, xy: 36},
		{x: 36, y: 162, xy: 57},
		{x: 136, y: 8, xy: 44},
		{x: 22, y: 64, xy: 247},
		{x: 210, y: 124, xy: 47},
		{x: 247, y: 27, xy: 18},
		{x: 224, y: 203, xy: 56},
		{x: 20, y: 228, xy: 127},
		{x: 194, y: 43, xy: 36},
		{x: 240, y: 50, xy: 75},
		{x: 186, y: 215, xy: 143},
		{x: 6, y: 20, xy: 120},
		{x: 243, y: 103, xy: 52},
		{x: 74, y: 228, xy: 151},
		{x: 183, y: 71, xy: 10},
		{x: 70, y: 136, xy: 102},
		{x: 243, y: 217, xy: 46},
		{x: 206, y: 149, xy: 25},
		{x: 230, y: 254, xy: 218},
		{x: 240, y: 153, xy: 12},
		{x: 252, y: 94, xy: 185},
		{x: 30, y: 255, xy: 228},
		{x: 57, y: 253, xy: 143},
		{x: 88, y: 253, xy: 223},
		{x: 136, y: 39, xy: 37},
		{x: 144, y: 248, xy: 209},
		{x: 47, y: 100, xy: 30},
		{x: 2, y: 154, xy: 47},
		{x: 210, y: 235, xy: 188},
		{x: 73, y: 52, xy: 32},
		{x: 105, y: 250, xy: 253},
		{x: 77, y: 81, xy: 55},
		{x: 130, y: 120, xy: 162},
		{x: 106, y: 201, xy: 67},
		{x: 246, y: 136, xy: 120},
		{x: 186, y: 241, xy: 148},
		{x: 102, y: 29, xy: 194},
		{x: 206, y: 83, xy: 86},
		{x: 99, y: 66, xy: 117},
		{x: 203, y: 162, xy: 165},
		{x: 185, y: 208, xy: 239},
		{x: 19, y: 206, xy: 29},
		{x: 156, y: 1, xy: 156},
		{x: 109, y: 99, xy: 166},
		{x: 14, y: 152, xy: 209},
		{x: 216, y: 65, xy: 100},
		{x: 82, y: 78, xy: 22},
		{x: 0, y: 57, xy: 0},
		{x: 77, y: 178, xy: 210},
		{x: 66, y: 18, xy: 200},
		{x: 25, y: 128, xy: 52},
		{x: 67, y: 52, xy: 243},
		{x: 94, y: 108, xy: 250},
		{x: 208, y: 160, xy: 74},
		{x: 1, y: 230, xy: 230},
		{x: 52, y: 135, xy: 201},
		{x: 39, y: 243, xy: 166},
		{x: 160, y: 94, xy: 225},
		{x: 58, y: 46, xy: 86},
		{x: 200, y: 165, xy: 22},
		{x: 121, y: 66, xy: 155},
		{x: 214, y: 196, xy: 70},
		{x: 71, y: 225, xy: 239},
		{x: 25, y: 179, xy: 153},
		{x: 225, y: 52, xy: 4},
		{x: 185, y: 44, xy: 167},
		{x: 57, y: 7, xy: 175},
		{x: 87, y: 135, xy: 134},
		{x: 204, y: 28, xy: 83},
		{x: 28, y: 72, xy: 161},
		{x: 74, y: 88, xy: 183},
		{x: 187, y: 160, xy: 148},
		{x: 87, y: 214, xy: 202},
		{x: 137, y: 28, xy: 126},
	}
)

func TestFiniteFieldMultiply(t *testing.T) {
	// to get the same result as pyfinite, we need to use primitive polynomial=283
	t.Log("Running test TestFiniteFieldMultiply")
	for _, c := range cases {
		assert.Equal(t, util.FiniteFieldMultiply(c.x, c.y, 0b100011011), c.xy, "Mulplication result should be the sane as pyfinite")
	}
}

func TestFiniteFieldInvert(t *testing.T) {
	for _, c := range cases {
		x := c.x
		yInv1 := util.FiniteFieldInvert(int16(c.y), 0b100011011, true)  // iterative calculation
		yInv2 := util.FiniteFieldInvert(int16(c.y), 0b100011011, false) // recursive calculation
		assert.Equal(t, yInv1, yInv2, "Iterative and recursive calculation of inverse should be the same")
		assert.Equal(t, util.FiniteFieldMultiply(c.y, byte(yInv1), 0b100011011), byte(1), "Product of a number and its inverse should be 1")
		assert.Equal(t, util.FiniteFieldMultiply(c.xy, byte(yInv1), 0b100011011), x, "x = (x . y) . 1 / y")
	}
}

func TestRecoverOneShardWithPShard(t *testing.T) {
	rs, err := New(dataShard)
	assert.NoError(t, err, "Creating encoder should succeed")
	length := rand.Intn(1000) + 1000
	bytes := make([]byte, length)
	_, err = rand.Read(bytes)
	assert.NoError(t, err, "Creating random bytes should succeed")
	dataShards := rs.Split([]byte(bytes))
	pShard, _, err := rs.Encode(dataShards)
	assert.NoError(t, err, "Computing P shard should succeed")
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(dataShards))
	dataShards[index] = nil
	err = rs.recoverOneShardWithPShard(dataShards, pShard, index)
	assert.NoError(t, err, "Recovering one shard with P shard should succeed")
	reconstructed := rs.Merge(dataShards)[:length]
	assert.Equal(t, bytes, reconstructed, "Recovered data should be the same as original data")
}

func TestRecoverOneShardWithQShard(t *testing.T) {
	rs, err := New(dataShard)
	assert.NoError(t, err, "Creating encoder should succeed")
	length := rand.Intn(1000) + 1000
	bytes := make([]byte, length)
	_, err = rand.Read(bytes)
	assert.NoError(t, err, "Creating random bytes should succeed")
	dataShards := rs.Split([]byte(bytes))
	_, qShard, err := rs.Encode(dataShards)
	assert.NoError(t, err, "Computing Q shard should succeed")
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(dataShards))
	dataShards[index] = nil
	err = rs.recoverOneShardWithQShard(dataShards, qShard, index)
	assert.NoError(t, err, "Recovering one shard with Q shard should succeed")
	reconstructed := rs.Merge(dataShards)[:length]
	assert.Equal(t, bytes, reconstructed, "Recovered data should be the same as original data")
}

func TestRecoverTwoShards(t *testing.T) {
	rs, err := New(dataShard)
	assert.NoError(t, err, "Creating encoder should succeed")
	length := rand.Intn(1000) + 1000
	bytes := make([]byte, length)
	_, err = rand.Read(bytes)
	assert.NoError(t, err, "Creating random bytes should succeed")
	dataShards := rs.Split([]byte(bytes))
	pShard, qShard, err := rs.Encode(dataShards)
	assert.NoError(t, err, "Computing P shard and Q shard should succeed")
	rand.Seed(time.Now().UnixNano())
	index1 := rand.Intn(len(dataShards))
	dataShards[index1] = nil
	index2 := index1
	for index2 == index1 {
		index2 = rand.Intn(len(dataShards))
	}
	dataShards[index1] = nil
	dataShards[index2] = nil
	if index2 > index1 {
		err = rs.recoverTwoShards(dataShards, pShard, qShard, index1, index2)
	} else {
		err = rs.recoverTwoShards(dataShards, pShard, qShard, index2, index1)
	}
	assert.NoError(t, err, "Recovering two shards should succeed")
	reconstructed := rs.Merge(dataShards)[:length]
	assert.Equal(t, bytes, reconstructed, "Recovered data should be the same as original data")
}
