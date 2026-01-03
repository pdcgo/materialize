package main

import (
	"log"

	"github.com/cespare/xxhash"
	"github.com/wargasipil/stream_engine/stream_core"
)

const (
	EntrySize = 16      // bytes per record
	TableSize = 1 << 20 // 1M slots
)

func offsetFor(key string) int64 {
	h := xxhash.Sum64String(key)
	slot := h & (TableSize - 1)
	return int64(slot * EntrySize)
}

func main() {
	// offset := offsetFor("teams/66/products/15962/warehouses/default/stock_count")
	// log.Println(offset)

	cfg := stream_core.NewDefaultCoreConfigTest()
	// dynamic, err := stream_core.NewDynamicValue(cfg)
	// if err != nil {
	// 	panic(err)
	// }
	// defer dynamic.Close()

	// offset, err = dynamic.Write("teams/66/products/15962/warehouses/default/stock_count", []byte("test dynamic value"))
	// log.Println(offset, err)

	// key, data := dynamic.Get(offset)
	// log.Printf("key %s, data %s\n", key, data)

	hashmap, err := stream_core.NewHashMapCounter(cfg)
	if err != nil {
		panic(err)
	}

	delta := hashmap.IncInt64("teams/66/products/15962/warehouses/default/stock_count", 2)
	log.Println("delta", delta)

}
