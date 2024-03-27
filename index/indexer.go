package index

import (
	"bytes"
	"fmt"
	"github.com/hello-ikun/ikunDb/storage"
	"sync"

	"github.com/elliotchance/orderedmap"
	"github.com/google/btree"
	art "github.com/plar/go-adaptive-radix-tree"
)

// IndexerType common
type IndexerType = uint8

const (
	None IndexerType = iota
	BTree
	OrderMap
	Art
	Base
)

var Indexers = map[IndexerType]IndexInterface{
	None:     NewBaseMap(),
	BTree:    NewBtreeMap(),
	OrderMap: NewOrderKvMap(),
	Art:      NewAdaptiveRadixTree(),
	Base:     NewBaseKvMap(),
}

// IndexInterface defines methods for setting, getting, and deleting key-value pairs.
type IndexInterface interface {
	Set(key []byte, value *storage.Pos) (*storage.Pos, bool)
	Get(key []byte) (*storage.Pos, error)
	Del(key []byte) (*storage.Pos, bool)
	Close() error
	Size() int64
	Iterator() Iterator
}

type Iterator interface {
	Head()
	Seek()
	Next()
	Key() []byte
	Value() *storage.Pos
	Valid() bool
	Close() error
}

type Items struct {
	key   []byte
	Value *storage.Pos
}

// achieve less method
func (ai *Items) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Items).key) == -1
}

type BaseKvMap struct {
	kvMap map[string]*storage.Pos
	m     *sync.RWMutex
}

func NewBaseKvMap() IndexInterface {
	return &BaseKvMap{kvMap: make(map[string]*storage.Pos), m: &sync.RWMutex{}}
}
func (b *BaseKvMap) Set(key []byte, value *storage.Pos) (*storage.Pos, bool) {
	b.m.Lock()
	defer b.m.Unlock()
	if old, ok := b.kvMap[string(key)]; ok {
		b.kvMap[string(key)] = value
		return old, true
	}
	b.kvMap[string(key)] = value
	return nil, true
}
func (b *BaseKvMap) Get(key []byte) (*storage.Pos, error) {
	b.m.RLock()
	defer b.m.RUnlock()
	if old, ok := b.kvMap[string(key)]; ok {
		return old, nil
	}
	return nil, storage.ErrKeyNotFound
}
func (b *BaseKvMap) Del(key []byte) (*storage.Pos, bool) {
	b.m.Lock()
	defer b.m.Unlock()
	if old, ok := b.kvMap[string(key)]; ok {
		delete(b.kvMap, string(key))
		return old, true
	}
	return nil, false
}
func (b *BaseKvMap) Close() error {
	return nil
}
func (b *BaseKvMap) Size() int64 {
	return int64(len(b.kvMap))
}
func (b *BaseKvMap) Iterator() Iterator {
	values := []*Items{}
	size := len(b.kvMap)
	for k, v := range b.kvMap {
		values = append(values, &Items{[]byte(k), v})
	}
	return &IteratorStruct{Values: values, Index: 0, Size: int64(size)}
}

// BaseMap is a concrete implementation of IndexInterface using sync.Map.
type BaseMap struct {
	m *sync.Map
	s int64
}

// NewBaseMap creates a new instance of BaseMap initialized with an empty sync.Map.
func NewBaseMap() IndexInterface {
	return &BaseMap{
		m: &sync.Map{},
	}
}

// Set adds or updates the key-value pair in the map.
func (b *BaseMap) Set(key []byte, value *storage.Pos) (*storage.Pos, bool) {
	fmt.Println(string(key), value)
	old, ok := b.m.LoadOrStore(string(key), value)
	if ok {
		return old.(*storage.Pos), true
	}
	b.s++
	return nil, true
}

// Get retrieves the value associated with the given key.
func (b *BaseMap) Get(key []byte) (*storage.Pos, error) {
	fmt.Println(string(key))
	// check data is exist
	val, ok := b.m.Load(string(key))
	if !ok {
		return nil, storage.ErrKeyNotFound
	}
	return val.(*storage.Pos), nil
}

// Del deletes the key-value pair associated with the given key.
func (b *BaseMap) Del(key []byte) (*storage.Pos, bool) {
	if old, ok := b.m.LoadAndDelete(string(key)); ok {
		b.s--
		return old.(*storage.Pos), true
	}
	return nil, false
}

func (b *BaseMap) Close() error {
	return nil
}
func (b *BaseMap) Size() int64 {
	return b.s
}
func (b *BaseMap) Iterator() Iterator {
	return b.baseMapIterator()
}
func (b *BaseMap) baseMapIterator() *IteratorStruct {
	values := []*Items{}
	var i int64
	b.m.Range(func(key, value any) bool {
		values = append(values, &Items{key: []byte(key.(string)), Value: value.(*storage.Pos)})
		i++
		return true
	})
	return &IteratorStruct{
		Values: values,
		Index:  0,
		Size:   i,
	}
}

// BtreeMap**********************************
type BtreeMap struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBtreeMap 新建 BtreeMap 索引结构
func NewBtreeMap() IndexInterface {
	return &BtreeMap{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BtreeMap) Set(key []byte, pos *storage.Pos) (*storage.Pos, bool) {
	it := &Items{key: key, Value: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, true
	}
	return oldItem.(*Items).Value, true
}

func (bt *BtreeMap) Get(key []byte) (*storage.Pos, error) {
	it := &Items{key: key}
	BtreeMapItem := bt.tree.Get(it)
	if BtreeMapItem == nil {
		return nil, storage.ErrKeyNotFound
	}
	return BtreeMapItem.(*Items).Value, nil
}

func (bt *BtreeMap) Del(key []byte) (*storage.Pos, bool) {
	it := &Items{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Items).Value, true
}

func (bt *BtreeMap) Size() int64 {
	return int64(bt.tree.Len())
}

func (bt *BtreeMap) Iterator() Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeMapIterator(bt.tree)
}

func (bt *BtreeMap) Close() error {
	return nil
}

func newBtreeMapIterator(tree *btree.BTree) *IteratorStruct {
	var idx int64
	values := []*Items{}

	// 将所有的数据存放到数组中
	tree.Ascend(func(item btree.Item) bool {
		values = append(values, item.(*Items))
		idx++
		return true
	})
	return &IteratorStruct{Values: values, Index: 0, Size: idx}
}

// AdaptiveRadixTree **********************************
type AdaptiveRadixTree struct {
	tree art.Tree
	lock *sync.RWMutex
}

func NewAdaptiveRadixTree() IndexInterface {
	return &AdaptiveRadixTree{
		tree: art.New(),
		lock: new(sync.RWMutex),
	}
}
func (art *AdaptiveRadixTree) Set(key []byte, pos *storage.Pos) (*storage.Pos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	old, _ := art.tree.Insert(key, pos)
	if old == nil {
		return nil, true
	}
	return old.(*storage.Pos), true
}
func (art *AdaptiveRadixTree) Get(key []byte) (*storage.Pos, error) {
	art.lock.RLock()
	defer art.lock.RUnlock()

	value, ok := art.tree.Search(key)
	if !ok {
		return nil, nil
	}
	return value.(*storage.Pos), nil
}
func (art *AdaptiveRadixTree) Del(key []byte) (*storage.Pos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	old, ok := art.tree.Delete(key)
	if old == nil {
		return nil, false
	}
	return old.(*storage.Pos), ok
}
func (art *AdaptiveRadixTree) Size() int64 {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return int64(art.tree.Size())
}
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

type OrderKVMap struct {
	OrderKVMap *orderedmap.OrderedMap
	mu         *sync.RWMutex
}

func NewOrderKvMap() IndexInterface {
	return &OrderKVMap{
		OrderKVMap: orderedmap.NewOrderedMap(),
		mu:         new(sync.RWMutex),
	}
}

func (om *OrderKVMap) Set(key []byte, value *storage.Pos) (*storage.Pos, bool) {
	om.mu.Lock()
	defer om.mu.Unlock()
	var oldPos *storage.Pos
	if val, exists := om.OrderKVMap.Get(string(key)); exists {
		oldPos = val.(*storage.Pos)
	}
	om.OrderKVMap.Set(string(key), value)
	return oldPos, true
}
func (om *OrderKVMap) Del(key []byte) (*storage.Pos, bool) {
	om.mu.Lock()
	defer om.mu.Unlock()
	var oldPos *storage.Pos
	if val, exists := om.OrderKVMap.Get(string(key)); exists {
		oldPos = val.(*storage.Pos)
	}
	return oldPos, om.OrderKVMap.Delete(string(key))
}

func (om *OrderKVMap) Get(key []byte) (*storage.Pos, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if val, exists := om.OrderKVMap.Get(string(key)); exists {
		return val.(*storage.Pos), nil
	}
	return nil, storage.ErrKeyNotFound
}

func (om *OrderKVMap) Size() int64 {
	om.mu.RLock()
	defer om.mu.RUnlock()

	return int64(om.OrderKVMap.Len())
}

func (om *OrderKVMap) Close() error {
	om.mu.Lock()
	defer om.mu.Unlock()

	om.OrderKVMap = nil
	return nil
}
func (om *OrderKVMap) Iterator() Iterator {
	return newOrderKVMapIterator(om.OrderKVMap)
}

func newOrderKVMapIterator(om *orderedmap.OrderedMap) *IteratorStruct {
	values := []*Items{}
	var index int64
	for em := om.Front(); em != nil; em = em.Next() {
		values = append(values, &Items{[]byte(em.Key.(string)), em.Value.(*storage.Pos)})
		index++
	}
	return &IteratorStruct{Values: values, Index: 0, Size: index}
}

// Iterator set iterator
func (art *AdaptiveRadixTree) Iterator() Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree)
}

func newArtIterator(tree art.Tree) Iterator {
	size := tree.Size()
	//set key and value
	values := []*Items{}
	saveValues := func(node art.Node) bool {
		items := &Items{
			key:   node.Key(),
			Value: node.Value().(*storage.Pos),
		}
		values = append(values, items)
		return true
	}
	tree.ForEach(saveValues)
	return &IteratorStruct{
		Index:  0,
		Size:   int64(size),
		Values: values,
	}
}

// IteratorStruct achieve
type IteratorStruct struct {
	Values []*Items
	Index  int64
	Size   int64
}

func (b *IteratorStruct) Head() {
	b.Index = 0
}
func (b *IteratorStruct) Seek() {
	return
}
func (b *IteratorStruct) Key() []byte {
	return b.Values[b.Index].key
}
func (b *IteratorStruct) Value() *storage.Pos {
	return b.Values[b.Index].Value
}
func (b *IteratorStruct) Next() {
	b.Index++
}
func (b *IteratorStruct) Valid() bool {
	return b.Index < b.Size
}
func (b *IteratorStruct) Close() error {
	return nil
}
