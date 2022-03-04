package batch

import "github.com/aws/aws-sdk-go/service/kinesis"

type noopAggregator struct {
}

func NewNoopAggregator() Aggregator {
	return &noopAggregator{}
}

func (a *noopAggregator) Drain() (*kinesis.PutRecordsRequestEntry, error) {
	return nil, nil
}
func (a *noopAggregator) Put(data []byte, partitionKey string) {
}
func (a *noopAggregator) IsRecordAggregative(data []byte, partitionKey string) bool {
	return false
}
func (a *noopAggregator) CheckIfFull(data []byte, partitionKey string) bool {
	return false
}
