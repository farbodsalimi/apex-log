// Package kinesis implements a batching AWS Kinesis handler.
package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/rogpeppe/fastuuid"
	buffer "github.com/tj/go-buffer"

	"github.com/apex/log"
)

// maxRecordsPerRequest is the Kinesis PutRecords hard limit.
const maxRecordsPerRequest = 500

// PutRecordsAPI abstracts the Kinesis PutRecords call for testing.
type PutRecordsAPI interface {
	PutRecords(ctx context.Context, in *kinesis.PutRecordsInput, opts ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}

// Handler implementation.
type Handler struct {
	stream string
	client PutRecordsAPI
	buffer *buffer.Buffer
	gen    *fastuuid.Generator
}

// New handler sending logs to Kinesis using the default AWS config.
func New(stream string) *Handler {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(fmt.Errorf("kinesis: load aws config: %w", err))
	}
	return NewWithClient(stream, kinesis.NewFromConfig(cfg))
}

// NewWithConfig handler using the provided aws.Config.
func NewWithConfig(stream string, cfg aws.Config) *Handler {
	return NewWithClient(stream, kinesis.NewFromConfig(cfg))
}

// NewWithClient handler using the provided Kinesis client (or any type satisfying PutRecordsAPI).
func NewWithClient(stream string, client PutRecordsAPI) *Handler {
	h := &Handler{
		stream: stream,
		client: client,
		gen:    fastuuid.MustNewGenerator(),
	}
	h.buffer = buffer.New(
		buffer.WithMaxEntries(maxRecordsPerRequest),
		buffer.WithFlushHandler(h.flush),
	)
	return h
}

// Close flushes pending records and stops the background flusher.
func (h *Handler) Close() error {
	h.buffer.Close()
	return nil
}

// HandleLog implements log.Handler.
func (h *Handler) HandleLog(e *log.Entry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	uuid := h.gen.Next()
	key := base64.StdEncoding.EncodeToString(uuid[:])
	h.buffer.Push(types.PutRecordsRequestEntry{
		Data:         b,
		PartitionKey: aws.String(key),
	})
	return nil
}

// flush sends buffered records to Kinesis.
func (h *Handler) flush(ctx context.Context, values []interface{}) error {
	records := make([]types.PutRecordsRequestEntry, len(values))
	for i, v := range values {
		records[i] = v.(types.PutRecordsRequestEntry)
	}
	_, err := h.client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(h.stream),
		Records:    records,
	})
	return err
}
