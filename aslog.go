// Package aslog provides a slog.Handler implementation for VictoriaLogs.
package aslog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Config defines the configuration for VLHandler.
type Config struct {
	URL           string        // VictoriaLogs URL
	BatchSize     int           // Maximum number of logs in a batch
	FlushInterval time.Duration // Maximum time between flushes
	BufferBytes   int           // Maximum size of the internal buffer, in bytes
	MaxRetries    int           // Maximum number of retry attempts
	Timeout       time.Duration // HTTP request timeout
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(url string) Config {
	return Config{
		URL:           url,
		BatchSize:     100,
		FlushInterval: time.Second,
		BufferBytes:   1000,
		MaxRetries:    3,
		Timeout:       5 * time.Second,
	}
}

// Validate checks if the config is valid.
func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("aslog: URL cannot be empty")
	}
	if c.BatchSize <= 0 {
		return errors.New("aslog: BatchSize must be positive")
	}
	if c.FlushInterval <= 0 {
		return errors.New("aslog: FlushInterval must be positive")
	}
	if c.BufferBytes <= 0 {
		return errors.New("aslog: BufferBytes must be positive")
	}
	if c.MaxRetries < 0 {
		return errors.New("aslog: MaxRetries cannot be negative")
	}
	if c.Timeout <= 0 {
		return errors.New("aslog: Timeout must be positive")
	}
	return nil
}

// VLHandler is a slog.Handler that sends logs to VictoriaLogs.
type VLHandler struct {
	cfg             Config
	client          *http.Client
	buffer          *buffer
	wg              sync.WaitGroup
	done            chan struct{}
	overflowCh      chan struct{}
	streamfields    map[string]string
	streamfieldsraw string

	// Immutable after creation
	baseAttrs []slog.Attr
	groups    []string
}

// NewVLHandler creates a new VLHandler with the given config.
// The handler must be closed with Close() to ensure all logs are flushed.
func NewVLHandler(cfg Config) (*VLHandler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	h := &VLHandler{
		cfg:             cfg,
		client:          &http.Client{Timeout: cfg.Timeout},
		buffer:          newBuffer(cfg.BufferBytes),
		done:            make(chan struct{}),
		overflowCh:      make(chan struct{}, 1),
		streamfields:    make(map[string]string, 0),
		streamfieldsraw: "",
	}

	h.wg.Add(1)
	go h.worker()

	return h, nil
}

func (h *VLHandler) AddStreamField(key string, value string) {
	h.streamfields[key] = value

	if h.streamfieldsraw != "" {
		h.streamfieldsraw += ","
	}
	h.streamfieldsraw += key

	fmt.Println(h.streamfieldsraw)
	fmt.Println(h.streamfields)
}

// Enabled reports whether the handler handles records at the given level.
func (h *VLHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

// Handle handles the Record.
func (h *VLHandler) Handle(_ context.Context, r slog.Record) error {

	// log.Println(h.streamfieldsraw)
	// log.Println(h.streamfields)

	// Pre-allocate map with expected capacity
	fields := make(map[string]any, 3+len(h.baseAttrs)+r.NumAttrs())

	// Standard fields
	fields["_time"] = r.Time.Format(time.RFC3339Nano)
	fields["level"] = r.Level.String()
	fields["_msg"] = r.Message

	// Static stream fields from config
	for k, v := range h.streamfields {
		fields[k] = v
	}

	// Base attributes (from WithAttrs)
	for _, attr := range h.baseAttrs {
		h.addAttr(fields, attr)
	}

	// Record attributes
	r.Attrs(func(a slog.Attr) bool {
		h.addAttr(fields, a)
		return true
	})

	// Serialize
	data, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("aslog: marshal error: %w", err)
	}

	// If buffer overflows, send the batch and notify the worker
	if h.buffer.Write(data) {
		select {
		case h.overflowCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (h *VLHandler) addAttr(fields map[string]any, attr slog.Attr) {
	key := attr.Key

	// Support for groups: group1.group2.key
	if len(h.groups) > 0 {
		var buf bytes.Buffer
		for i, g := range h.groups {
			if i > 0 {
				buf.WriteByte('.')
			}
			buf.WriteString(g)
		}
		buf.WriteByte('.')
		buf.WriteString(attr.Key)
		key = buf.String()
	}

	// Handle nested groups in values
	val := attr.Value.Resolve()
	if val.Kind() == slog.KindGroup {
		for _, groupAttr := range val.Group() {
			h.addAttr(fields, slog.Attr{
				Key:   key + "." + groupAttr.Key,
				Value: groupAttr.Value,
			})
		}
		return
	}

	fields[key] = val.Any()
}

// WithAttrs returns a new Handler with additional attributes.
func (h *VLHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	newAttrs := make([]slog.Attr, len(h.baseAttrs)+len(attrs))
	copy(newAttrs, h.baseAttrs)
	copy(newAttrs[len(h.baseAttrs):], attrs)

	newGroups := make([]string, len(h.groups))
	copy(newGroups, h.groups)

	return &VLHandler{
		cfg:             h.cfg,
		client:          h.client,
		buffer:          h.buffer,
		done:            h.done,
		baseAttrs:       newAttrs,
		groups:          newGroups,
		streamfields:    h.streamfields,
		streamfieldsraw: h.streamfieldsraw,
	}
}

// WithGroup returns a new Handler with the given group name.
func (h *VLHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	newGroups := make([]string, len(h.groups)+1)
	copy(newGroups, h.groups)
	newGroups[len(h.groups)] = name

	newAttrs := make([]slog.Attr, len(h.baseAttrs))
	copy(newAttrs, h.baseAttrs)

	return &VLHandler{
		cfg:             h.cfg,
		client:          h.client,
		buffer:          h.buffer,
		done:            h.done,
		baseAttrs:       newAttrs,
		groups:          newGroups,
		streamfields:    h.streamfields,
		streamfieldsraw: h.streamfieldsraw,
	}
}

func (h *VLHandler) worker() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.overflowCh:
			go h.flushAndSend()

		case <-ticker.C:
			go h.flushAndSend()

		case <-h.done:
			return
		}
	}
}

func (h *VLHandler) sendBatch(batch [][]byte) {
	// Build body: JSON lines (ndjson)
	body := bytes.Join(batch, []byte("\n"))

	// Compress body with gzip
	var compressedBuf bytes.Buffer
	gz := gzip.NewWriter(&compressedBuf)
	if _, err := gz.Write(body); err != nil {
		return
	}
	if err := gz.Close(); err != nil {
		return
	}
	compressedBody := compressedBuf.Bytes()

	// Retry with exponential backoff
	for attempt := 0; attempt < h.cfg.MaxRetries; attempt++ {
		req, err := http.NewRequest("POST", h.cfg.URL, bytes.NewReader(compressedBody))
		if err != nil {
			continue
		}

		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Content-Length", strconv.Itoa(len(compressedBody)))

		req.Header.Set("VL-Stream-Fields", h.streamfieldsraw)

		fmt.Println("VL-Stream-Fields", h.streamfieldsraw)

		resp, err := h.client.Do(req)
		if err != nil {
			select {
			case <-time.After(time.Duration(attempt+1) * time.Second):
				continue
			case <-h.done:
				return
			case <-h.overflowCh:
				return
			}
		}

		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return
		}

		// Retry on 5xx errors, but not on 4xx
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return
		}

		return
	}
}

func (h *VLHandler) flushAndSend() {
	batch := h.buffer.Flush()
	if len(batch) > 0 {
		h.sendBatch(batch)
	}
}

// Close flushes remaining logs and shuts down the handler.
// It blocks until all logs are sent or the context is cancelled.
func (h *VLHandler) Close(ctx context.Context) error {
	close(h.done)

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
