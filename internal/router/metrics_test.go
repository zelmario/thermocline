package router

import (
	"testing"
	"time"
)

func TestQuantileNeverExceedsMax(t *testing.T) {
	m := NewMetrics(10)
	for i := 0; i < 28; i++ {
		m.Record(ClassArchive, msDur(1400))
	}
	s := m.Snapshot()
	c := s.Classes[ClassArchive]
	if c.P50MS > c.MaxMS || c.P95MS > c.MaxMS || c.P99MS > c.MaxMS {
		t.Fatalf("percentile above max: p50=%v p95=%v p99=%v max=%v", c.P50MS, c.P95MS, c.P99MS, c.MaxMS)
	}
	if c.Count != 28 {
		t.Fatalf("count = %d, want 28", c.Count)
	}
}

func msDur(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }
