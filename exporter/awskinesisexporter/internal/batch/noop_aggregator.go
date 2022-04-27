package batch

type noopAggregator struct {
}

func NewNoopAggregator() Aggregator {
	return &noopAggregator{}
}
func (*noopAggregator) SetMaxRecordSize(size int) {
}
func (a *noopAggregator) Drain() ([]byte, error) {
	return nil, nil
}
func (a *noopAggregator) Put(data []byte, partitionKey string) ([]byte, error) {
	return data, nil
}
func (a *noopAggregator) IsRecordAggregative(data []byte, partitionKey string) bool {
	return false
}
func (a *noopAggregator) CheckIfFull(data []byte, partitionKey string) bool {
	return false
}
