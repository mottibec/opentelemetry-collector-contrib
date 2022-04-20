package batch

import (
	"crypto/md5"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"google.golang.org/protobuf/proto"
)

var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

const (
	partitionKeyIndexSize = 8
	maxAggregationCount   = 4294967295
	maxRecordSize         = 1 << 20
)

type Aggregator interface {
	Drain() (*kinesis.PutRecordsRequestEntry, error)
	Put(data []byte, partitionKey string)
	IsRecordAggregative(data []byte, partitionKey string) bool
	CheckIfFull(data []byte, partitionKey string) bool
}

type aggregator struct {
	records       []*Record
	partitionKeys []string
	bytesCount    int
}

func NewAggregator() Aggregator {
	return &aggregator{}
}

func (a *aggregator) IsRecordAggregative(data []byte, partitionKey string) bool {
	return len(data)+len([]byte(partitionKey)) <= maxRecordSize
}

func (a *aggregator) CheckIfFull(data []byte, partitionKey string) bool {
	bytesCount := len(data) + len([]byte(partitionKey))
	return bytesCount+a.bytesCount+md5.Size+len(magicNumber)+partitionKeyIndexSize > maxRecordSize || len(a.records) >= maxAggregationCount
}

func (a *aggregator) Put(data []byte, partitionKey string) {
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

func (a *aggregator) Drain() (*kinesis.PutRecordsRequestEntry, error) {
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
	entry := &kinesis.PutRecordsRequestEntry{
		Data:         aggData,
		PartitionKey: &a.partitionKeys[0],
	}
	a.clear()
	return entry, nil
}

func (a *aggregator) clear() {
	a.records = a.records[:0]
	a.partitionKeys = a.partitionKeys[:0]
	a.bytesCount = 0
}
