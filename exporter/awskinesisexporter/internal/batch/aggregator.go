package batch

import (
	"crypto/md5"

	"google.golang.org/protobuf/proto"
)

var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

const (
	partitionKeyIndexSize = 8
	maxAggregationCount   = 4294967295
	maxRecordSize         = 1 << 20
)

type Aggregator interface {
	SetMaxRecordSize(size int)
	Put(data []byte, partitionKey string) ([]byte, error)
	Drain() ([]byte, error)
}

type aggregator struct {
	records       []*Record
	partitionKeys []string
	bytesCount    int
	maxRecordSize int
}

func NewAggregator() Aggregator {
	return &aggregator{
		maxRecordSize: maxRecordSize,
	}
}

func (a *aggregator) SetMaxRecordSize(size int) {
	a.maxRecordSize = size
}

func (a *aggregator) isRecordAggregative(data []byte, partitionKey string) bool {
	return len(data)+len([]byte(partitionKey)) <= a.maxRecordSize
}

func (a *aggregator) checkIfFull(data []byte, partitionKey string) bool {
	bytesCount := len(data) + len([]byte(partitionKey))
	return bytesCount+a.bytesCount+md5.Size+len(magicNumber)+partitionKeyIndexSize > a.maxRecordSize || len(a.records) >= maxAggregationCount
}

func (a *aggregator) aggregate(data []byte, partitionKey string) {
	if len(a.partitionKeys) == 0 {
		a.partitionKeys = []string{partitionKey}
		a.bytesCount += len([]byte(partitionKey))
	}
	keyIndex := uint64(len(a.partitionKeys) - 1)

	a.bytesCount += partitionKeyIndexSize
	a.records = append(a.records, &Record{
		Data:              data,
		PartitionKeyIndex: &keyIndex,
	})
	a.bytesCount += len(data)
}

func (a *aggregator) Put(data []byte, partitionKey string) ([]byte, error) {
	if a.isRecordAggregative(data, partitionKey) {
		if a.checkIfFull(data, partitionKey) {
			record, err := a.Drain()
			if err != nil {
				return nil, err
			}
			if record != nil {
				return record, nil
			}
		} else {
			a.aggregate(data, partitionKey)
			return nil, nil
		}
	}
	return data, nil
}

func (a *aggregator) Drain() ([]byte, error) {
	if a.bytesCount == 0 {
		return nil, nil
	}
	data, err := proto.Marshal(&AggregatedRecord{
		PartitionKeyTable: a.partitionKeys,
		Records:           a.records,
	})
	if err != nil {
		return nil, err
	}
	h := md5.New()
	h.Write(data)
	checkSum := h.Sum(nil)
	aggData := append(magicNumber, data...)
	aggData = append(aggData, checkSum...)
	defer a.clear()
	return aggData, nil
}

func (a *aggregator) clear() {
	a.records = a.records[:0]
	a.partitionKeys = a.partitionKeys[:0]
	a.bytesCount = 0
}
