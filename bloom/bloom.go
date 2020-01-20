package bloom

import (
	"github.com/go-redis/redis"

	"fmt"
	"hash/fnv"
	"math"
)

const redisMaxLength = 8 * 512 * 1024 * 1024

type RedisBitSet struct {
	keyPrefix string
	m         uint

	client redis.Cmdable
}

func NewRedisBitSet(keyPrefix string, m uint, client redis.Cmdable) *RedisBitSet {
	return &RedisBitSet{keyPrefix, m, client}
}

func (r *RedisBitSet) BatchSet(keyOffsetsM map[string][]uint) error {
	pipe := r.client.Pipeline()

	_, err := pipe.Pipelined(func(p redis.Pipeliner) error {
		for _, offsets := range keyOffsetsM {
			for _, offset := range offsets {
				key, thisOffset := r.getKeyOffset(offset)
				_, err := p.SetBit(key, int64(thisOffset), 1).Result()
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

func (r *RedisBitSet) BatchTest(keyOffsetsM map[string][]uint) (map[string]bool, error) {
	pipe := r.client.Pipeline()

	res := make(map[string]bool, len(keyOffsetsM))
	result := make(map[string][]*redis.IntCmd, 10)
	_, err := pipe.Pipelined(func(p redis.Pipeliner) error {
		for rawKey, offsets := range keyOffsetsM {
			for _, offset := range offsets {
				key, thisOffset := r.getKeyOffset(offset)
				cmd := p.GetBit(key, int64(thisOffset))

				if _, ok := result[rawKey]; !ok {
					result[rawKey] = make([]*redis.IntCmd, 0, len(offsets))
				}
				result[rawKey] = append(result[rawKey], cmd)

			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	for rawKey, cmdList := range result {
		var exists = true
		for _, cmd := range cmdList {
			ret, err := cmd.Result()
			if err != nil {
				return nil, err
			}

			if ret == 0 {
				exists = false
				break
			}
		}

		res[rawKey] = exists
	}

	return res, nil
}

func (r *RedisBitSet) getKeyOffset(offset uint) (string, uint) {
	n := uint(offset / redisMaxLength) // n==0
	thisOffset := offset - n*redisMaxLength
	key := r.getKey()
	return key, thisOffset
}

func (r *RedisBitSet) getKey() string {
	return fmt.Sprintf("%s:%d", r.keyPrefix, 0)
}

func (r *RedisBitSet) Init() error {
	_, err := r.client.SetBit(r.getKey(), int64(r.m)+1, 0).Result()

	return err
}

type BloomFilter struct {
	m      uint
	k      uint
	bitSet *RedisBitSet
}

func NewBloomFilter(m uint, k uint, bitSet *RedisBitSet) *BloomFilter {
	return &BloomFilter{m: m, k: k, bitSet: bitSet}
}

func EstimateParameters(n uint, p float64) (uint, uint) {
	m := math.Ceil(float64(n) * math.Log(p) / math.Log(1.0/math.Pow(2.0, math.Ln2)))
	k := math.Ln2*m/float64(n) + 0.5

	return uint(m), uint(k)
}

// 设置好最长的bitmap,不用扩容
func (f *BloomFilter) Init() error {
	return f.bitSet.Init()
}

func (f *BloomFilter) Add(keys ...string) error {
	keyOffsetsM := f.getKeyOffsetsM(keys...)
	err := f.bitSet.BatchSet(keyOffsetsM)
	if err != nil {
		return err
	}
	return nil
}

func (f *BloomFilter) Exists(keys ...string) (map[string]bool, error) {
	keyOffsetsM := f.getKeyOffsetsM(keys...)
	return f.bitSet.BatchTest(keyOffsetsM)
}

func (f *BloomFilter) getLocations(data []byte) []uint {
	locations := make([]uint, f.k)
	hasher := fnv.New64()
	hasher.Write(data)
	a := make([]byte, 1)
	for i := uint(0); i < f.k; i++ {
		a[0] = byte(i)
		hasher.Write(a)
		hashValue := hasher.Sum64()
		locations[i] = uint(hashValue % uint64(f.m))
	}
	return locations
}

func (f *BloomFilter) getKeyOffsetsM(keys ...string) map[string][]uint {
	keyOffsetsM := make(map[string][]uint, len(keys))
	for _, key := range keys {
		keyOffsetsM[key] = f.getLocations([]byte(key))
	}

	return keyOffsetsM
}
