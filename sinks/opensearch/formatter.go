package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/hashicorp/eventlogger"
	"time"
)

const (
	OpenSearchFormat = "json"
)

// OpenSearchFormatter is a Formatter Node which formats the Event in OpenSearch format.
type OpenSearchFormatter struct{}

var _ eventlogger.Node = &OpenSearchFormatter{}

// Process formats the Event as JSON and stores that formatted data in
// Event.Formatted with a key of "json"
func (w *OpenSearchFormatter) Process(ctx context.Context, e *eventlogger.Event) (*eventlogger.Event, error) {
	payloadBytes, err := json.Marshal(e.Payload)
	if err != nil {
		return nil, err
	}

	var payload map[string]interface{}
	if err = json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)

	err = enc.Encode(struct {
		CreatedAt             time.Time `json:"created_at"`
		eventlogger.EventType `json:"event_type"`
		Payload               interface{} `json:"payload"`
	}{
		e.CreatedAt,
		e.Type,
		payload,
	})
	if err != nil {
		return nil, err
	}

	e.FormattedAs(OpenSearchFormat, buf.Bytes())
	return e, nil
}

// Reopen is a no op
func (w *OpenSearchFormatter) Reopen() error {
	return nil
}

// Type describes the type of the node as a Formatter.
func (w *OpenSearchFormatter) Type() eventlogger.NodeType {
	return eventlogger.NodeTypeFormatter
}

// Name returns a representation of the Formatter's name
func (w *OpenSearchFormatter) Name() string {
	return "OpenSearchFormatter"
}
