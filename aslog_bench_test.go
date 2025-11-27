package aslog

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// BenchmarkVLHandlerHandle benchmarks the Handle method of VLHandler.
func BenchmarkVLHandlerHandle(b *testing.B) {
	// Create a handler instance tailored for benchmarking.
	// We use a large buffer to avoid overflows and network activity.
	h := &VLHandler{
		buffer:       newBuffer(1 << 30), // large buffer to avoid flushes
		overflowCh:   make(chan struct{}, 1),
		streamfields: [][2]string{{"app", "bench"}, {"env", "test"}},
		baseAttrs: []slog.Attr{
			slog.String("service", "benchmark"),
			slog.Int("version", 1),
		},
	}

	// Prepare a representative log record with several attributes and a group.
	r := slog.NewRecord(time.Now(), slog.LevelInfo, "benchmark message", 0)
	r.AddAttrs(
		slog.String("user", "alice"),
		slog.Int("count", 42),
		slog.Float64("float", 123.456),
		slog.Bool("bool", true),
		slog.Time("time", time.Now()),
		slog.Duration("duration", 100*time.Millisecond),
		slog.Group("meta",
			slog.String("ip", "127.0.0.1"),
			slog.Bool("admin", true),
		),
	)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.Handle(ctx, r); err != nil {
			b.Fatal(err)
		}
	}
}
