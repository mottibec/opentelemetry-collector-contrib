package batch_test

import (
	"bytes"
	"crypto/md5"
	"strconv"
	sync "sync"
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awskinesisexporter/internal/batch"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

func TestAggregation(t *testing.T) {
	var wg sync.WaitGroup
	a := batch.NewAggregator()
	n := 500
	wg.Add(n)
	var aggregated []byte
	var err error
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		data := []byte("hello-" + c)
		a.Put(data, c)
		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}
	wg.Wait()
	aggregated, err = a.Drain()
	if err != nil {
		t.Error(err)
	}
	isAggregated := bytes.HasPrefix(aggregated, []byte{0xF3, 0x89, 0x9A, 0xC2})
	assert.True(t, isAggregated, "Must have aggregated the data")
	records := extractRecords(aggregated)
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		found := false
		for _, record := range records {
			if string(record.Data) == "hello-"+c {
				assert.Equal(t, string(record.Data), "hello-"+c, "`Data` field contains invalid value")
				found = true
			}
		}
		assert.True(t, found, "record not found after extracting: "+c)
	}
}

func extractRecords(data []byte) (out []*kinesis.PutRecordsRequestEntry) {
	src := data[len(magicNumber) : len(data)-md5.Size]
	dest := new(batch.AggregatedRecord)
	err := proto.Unmarshal(src, dest)
	if err != nil {
		return
	}
	for i := range dest.Records {
		r := dest.Records[i]
		out = append(out, &kinesis.PutRecordsRequestEntry{
			Data:         r.GetData(),
			PartitionKey: &dest.PartitionKeyTable[r.GetPartitionKeyIndex()],
		})
	}
	return
}
