package router

import (
	"sync"
	"time"

	"github.com/zelmario/thermocline/internal/config"
)

// missMemoryCap bounds how many documents missMemory remembers. A document id
// is about sixty bytes here, so this is around twenty megabytes at worst.
const missMemoryCap = 200000

// missMemory remembers which documents a point read recently found only in the
// archive, so router.copy_up_reads: repeated can copy one up on its second miss
// rather than its first. The zero value is ready to use.
type missMemory struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// again reports whether docID missed before, inside window, and remembers this
// miss if not. Full, it forgets what is older than the window first, and
// everything if that is not enough: forgetting only makes a document wait for
// one more miss, which is the cost this setting already accepts.
func (m *missMemory) again(docID string, now time.Time, window time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if at, ok := m.seen[docID]; ok && now.Sub(at) <= window {
		delete(m.seen, docID)
		return true
	}
	if m.seen == nil {
		m.seen = map[string]time.Time{}
	}
	if len(m.seen) >= missMemoryCap {
		for id, at := range m.seen {
			if now.Sub(at) > window {
				delete(m.seen, id)
			}
		}
		if len(m.seen) >= missMemoryCap {
			m.seen = map[string]time.Time{}
		}
	}
	m.seen[docID] = now
	return false
}

// copyUpRead decides whether a point read that found docID only in the archive
// copies it up, under router.copy_up_reads.
//
// The read is answered from the archive either way, and a copy-up after a read
// runs behind the reply, so this decides nothing a client waits for directly.
// It decides what the fast tier and the archive spend: each copy-up is a write
// on the fast tier and the residency and echo bookkeeping on the archive.
// Measured on a 40 million order adoption with the default: 404,870 copy-ups
// in twelve minutes, most of them old orders read once by a long tail of
// lookups, and the throughput of the whole workload 36% below the same
// cluster without tiering.
func (r *Router) copyUpRead(docID string) bool {
	switch r.cfg.CopyUpReads {
	case config.CopyUpNever:
		return false
	case config.CopyUpRepeated:
		return r.missed.again(docID, time.Now(), r.cfg.CopyUpWindow.D())
	default:
		return true
	}
}
