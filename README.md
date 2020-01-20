# bloom_filter_redis


## how to use

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

