package opensearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/hashicorp/eventlogger"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"io"
)

type OpenSearchSink struct {
	// Format specifies the format the []byte representation is formatted in
	// Defaults to JSONFormat
	Format string

	// Client represents the OpenSearch client.
	//
	Client *opensearch.Client

	// IndexName represents the name of the index
	IndexName string
}

func (o OpenSearchSink) Process(ctx context.Context, e *eventlogger.Event) (*eventlogger.Event, error) {
	format := o.Format
	if format == "" {
		format = eventlogger.JSONFormat
	}
	val, ok := e.Format(format)
	if !ok {
		return nil, errors.New("event was not marshaled")
	}

	reader := bytes.NewReader(val)

	payloadDigest := sha256.Sum256(val)
	// Set up the request object.
	req := opensearchapi.IndexRequest{
		Index:      o.IndexName,
		Body:       reader,
		Refresh:    "true",
		DocumentID: fmt.Sprintf("%x", payloadDigest),
	}

	// Perform the request with the client.
	res, err := req.Do(ctx, o.Client)
	if err != nil {
		return nil, err
	}

	err = o.validateResultIsNotError(res)
	if err != nil {
		return nil, err
	}

	err = o.closeResponseBody(res)
	if err != nil {
		return nil, err
	}

	// Sinks are leafs, so do not return the event, since nothing more can
	// happen to it downstream.
	return nil, nil
}

// Reopen does nothing for this type of Sink.  They cannot be rotated.
func (o OpenSearchSink) Reopen() error {
	return nil
}

// Type defines the Sink as a NodeTypeSink
func (o OpenSearchSink) Type() eventlogger.NodeType {
	return eventlogger.NodeTypeSink
}

var _ eventlogger.Node = &OpenSearchSink{}

func (o OpenSearchSink) closeResponseBody(res *opensearchapi.Response) error {
	if res != nil {
		err := res.Body.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (o OpenSearchSink) validateResultIsNotError(resp *opensearchapi.Response) error {
	if !resp.IsError() {
		return nil
	}

	errorBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return errors.New(fmt.Sprintf("unhandled error (%s): %+v", resp.Status(), errorBody))
}
