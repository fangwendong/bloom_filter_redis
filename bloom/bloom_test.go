package bloom

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/elliotchance/redismock"
	"github.com/go-redis/redis"
)

func newTestRedis() redis.Cmdable {
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return redismock.NewNiceMock(client)
}

func TestEstimateParameters(t *testing.T) {
	m, k := EstimateParameters(uint(1e8), .01)
	fmt.Println(m, k)
}

func TestNewRedisBitSet(t *testing.T) {
	bs := NewRedisBitSet("prefix", 1000, newTestRedis())
	bs.Init()
}

func TestNewBloomFilter(t *testing.T) {
	m, k := EstimateParameters(uint(1e8), .01)
	bs := NewRedisBitSet("prefix", m, newTestRedis())
	bf := NewBloomFilter(m, k, bs)
	bf.Init()
}

func TestBloomFilter_Add(t *testing.T) {
	m, k := EstimateParameters(uint(1e8), .01)
	bs := NewRedisBitSet("prefix", m, newTestRedis())
	bf := NewBloomFilter(m, k, bs)
	bf.Init()
	testMap := map[string]bool{
		"t1":    true,
		"t2":    false,
		"xxxxx": true,
		"ssss":  true,
		"fff":   false,
	}

	keys := make([]string, 0, len(testMap))
	for key, v := range testMap {
		if v {
			err := bf.Add(key)
			if err != nil {
				panic(err)
			}
		}
		keys = append(keys, key)
	}

	ret, err := bf.Exists(keys...)
	if err != nil {
		panic(err)
	}

	for key, want := range testMap {
		real := ret[key]

		if want != real {
			t.Errorf("key:%s want:%v real:%v", key, want, real)
		}
	}

}
