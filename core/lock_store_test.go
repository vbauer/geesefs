package core

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// fakeLockBackend is an in-memory S3 that honours the conditional headers the
// lock protocol relies on (If-None-Match:* and If-Match:<etag>), returning 404
// for missing keys and 412 for failed preconditions — exactly what lockStore
// expects from real S3. This lets us prove the CAS-based "first writer wins"
// and reclaim/heartbeat logic without a live bucket.
type fakeLockBackend struct {
	mu   sync.Mutex
	objs map[string][]byte
	tags map[string]string
	seq  int
	gets int // round-trip counters (assert HEAD+GET collapse)
	puts int
}

func newFakeLockBackend() *fakeLockBackend {
	return &fakeLockBackend{objs: map[string][]byte{}, tags: map[string]string{}}
}

func (f *fakeLockBackend) counts() (gets, puts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets, f.puts
}

func reqFail(code string, status int) error {
	return awserr.NewRequestFailure(awserr.New(code, code, nil), status, "test-req")
}

func (f *fakeLockBackend) GetBlob(in *GetBlobInput) (*GetBlobOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	body, ok := f.objs[in.Key]
	if !ok {
		return nil, reqFail("NoSuchKey", 404)
	}
	etag := f.tags[in.Key]
	out := &GetBlobOutput{Body: io.NopCloser(bytes.NewReader(append([]byte(nil), body...)))}
	out.ETag = &etag
	return out, nil
}

func (f *fakeLockBackend) PutBlob(in *PutBlobInput) (*PutBlobOutput, error) {
	body, _ := io.ReadAll(in.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	cur, exists := f.tags[in.Key]
	if in.IfNoneMatch != nil && *in.IfNoneMatch == "*" && exists {
		return nil, reqFail("PreconditionFailed", 412)
	}
	if in.IfMatch != nil && (!exists || cur != *in.IfMatch) {
		return nil, reqFail("PreconditionFailed", 412)
	}
	f.seq++
	etag := fmt.Sprintf("etag-%d", f.seq)
	f.objs[in.Key] = body
	f.tags[in.Key] = etag
	return &PutBlobOutput{ETag: &etag}, nil
}

func newTestStore(b lockBackend, session, owner, client string, ttl time.Duration, clk *time.Time) *lockStore {
	return &lockStore{
		backend: b,
		id:      lockIdentity{session: session, owner: owner, client: client},
		ttl:     ttl,
		now:     func() time.Time { return *clk },
	}
}

func mustAcquire(t *testing.T, s *lockStore, key string) string {
	t.Helper()
	res, etag, err := s.tryAcquire(key)
	if err != nil {
		t.Fatalf("tryAcquire %s: unexpected error %v", key, err)
	}
	if res != lockAcquired {
		t.Fatalf("tryAcquire %s: expected acquired, got busy", key)
	}
	return etag
}

func TestStoreFirstWriterWins(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	a := newTestStore(b, "sess-a", "ivan", "host-a", 30*time.Minute, &now)
	c := newTestStore(b, "sess-b", "petr", "host-b", 30*time.Minute, &now)

	mustAcquire(t, a, "finance/Q1.xlsx")

	// Second mount must see busy and fail to acquire.
	busy, holder, err := c.busyByOther("finance/Q1.xlsx")
	if err != nil || !busy {
		t.Fatalf("expected busy, got busy=%v err=%v", busy, err)
	}
	if holder != "ivan" {
		t.Fatalf("expected holder ivan, got %q", holder)
	}
	if res, _, _ := c.tryAcquire("finance/Q1.xlsx"); res != lockBusy {
		t.Fatal("second mount must not acquire a held lock")
	}

	// Owner re-entry must succeed without taking it from itself.
	if res, _, _ := a.tryAcquire("finance/Q1.xlsx"); res != lockAcquired {
		t.Fatal("re-entry by the same session must succeed")
	}
}

func TestStoreReleaseFreesLock(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	a := newTestStore(b, "sess-a", "ivan", "host-a", 30*time.Minute, &now)
	c := newTestStore(b, "sess-b", "petr", "host-b", 30*time.Minute, &now)

	mustAcquire(t, a, "doc.txt")
	if err := a.release(lockKey("doc.txt")); err != nil {
		t.Fatalf("release: %v", err)
	}
	if busy, _, _ := c.busyByOther("doc.txt"); busy {
		t.Fatal("released lock must not be busy")
	}
	// And the next mount can take it (held:false is reclaimable).
	mustAcquire(t, c, "doc.txt")
}

func TestStoreStaleReclaimByTTL(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	a := newTestStore(b, "sess-a", "ivan", "host-a", 30*time.Minute, &now)
	c := newTestStore(b, "sess-b", "petr", "host-b", 30*time.Minute, &now)

	mustAcquire(t, a, "doc.txt")
	if busy, _, _ := c.busyByOther("doc.txt"); !busy {
		t.Fatal("fresh lock must be busy for others")
	}

	now = now.Add(31 * time.Minute) // expire it

	if busy, _, _ := c.busyByOther("doc.txt"); busy {
		t.Fatal("expired lock must not be busy")
	}
	mustAcquire(t, c, "doc.txt") // stale reclaim
}

func TestStoreSameHostReclaim(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	old := newTestStore(b, "sess-old", "ivan", "host-a", 30*time.Minute, &now)
	// New process on the SAME host/owner (remount) — reclaims even if not expired.
	remount := newTestStore(b, "sess-new", "ivan", "host-a", 30*time.Minute, &now)
	other := newTestStore(b, "sess-x", "ivan", "host-b", 30*time.Minute, &now)

	mustAcquire(t, old, "doc.txt")

	if busy, _, _ := other.busyByOther("doc.txt"); !busy {
		t.Fatal("same owner on a different host must still see busy")
	}
	if busy, _, _ := remount.busyByOther("doc.txt"); busy {
		t.Fatal("same host remount must be reclaimable (not busy)")
	}
	mustAcquire(t, remount, "doc.txt")
}

func TestStoreHeartbeatRenewAndLoss(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	a := newTestStore(b, "sess-a", "ivan", "host-a", 30*time.Minute, &now)
	c := newTestStore(b, "sess-b", "petr", "host-b", 30*time.Minute, &now)

	etag := mustAcquire(t, a, "doc.txt")

	// Normal heartbeat renews and returns a fresh etag.
	res, newEtag, err := a.renew(lockKey("doc.txt"), etag)
	if err != nil || res != lockAcquired {
		t.Fatalf("renew: res=%v err=%v", res, err)
	}
	if newEtag == "" || newEtag == etag {
		t.Fatalf("renew must return a new etag, got %q (was %q)", newEtag, etag)
	}

	// Someone reclaims after expiry; our next heartbeat must report a confirmed loss.
	now = now.Add(31 * time.Minute)
	mustAcquire(t, c, "doc.txt") // c takes over
	res, _, err = a.renew(lockKey("doc.txt"), newEtag)
	if err != nil {
		t.Fatalf("renew after reclaim: unexpected transient error %v", err)
	}
	if res != lockBusy {
		t.Fatal("renew must report lockBusy after the sidecar was reclaimed")
	}
}

func TestStoreNoSidecarNotBusy(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	a := newTestStore(b, "sess-a", "ivan", "host-a", 30*time.Minute, &now)
	if busy, _, err := a.busyByOther("never-locked.txt"); err != nil || busy {
		t.Fatalf("missing sidecar must be free, got busy=%v err=%v", busy, err)
	}
}

func TestStoreSingleRoundTrips(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	a := newTestStore(b, "sess-a", "ivan", "host-a", 30*time.Minute, &now)
	c := newTestStore(b, "sess-b", "petr", "host-b", 30*time.Minute, &now)

	// First acquire on a free key: one GET (404) + one PUT (create). No HEAD.
	mustAcquire(t, a, "doc.txt")
	if g, p := b.counts(); g != 1 || p != 1 {
		t.Fatalf("acquire on free key: want 1 GET + 1 PUT, got %d GET %d PUT", g, p)
	}

	// busyByOther on an existing sidecar: a single GET (no preceding HEAD).
	g0, p0 := b.counts()
	if busy, _, _ := c.busyByOther("doc.txt"); !busy {
		t.Fatal("expected busy")
	}
	g1, p1 := b.counts()
	if g1-g0 != 1 || p1-p0 != 0 {
		t.Fatalf("busyByOther: want +1 GET +0 PUT, got +%d GET +%d PUT", g1-g0, p1-p0)
	}
}

func TestStoreTryCreate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	a := newTestStore(b, "sess-a", "ivan", "host-a", 30*time.Minute, &now)
	c := newTestStore(b, "sess-b", "petr", "host-b", 30*time.Minute, &now)

	// Optimistic create on a truly free key: a single PUT, no GET.
	res, etag, err := a.tryCreate("doc.txt")
	if err != nil || res != lockAcquired || etag == "" {
		t.Fatalf("tryCreate free: res=%v etag=%q err=%v", res, etag, err)
	}
	if g, p := b.counts(); g != 0 || p != 1 {
		t.Fatalf("tryCreate free: want 0 GET + 1 PUT, got %d GET %d PUT", g, p)
	}

	// Optimistic create when a sidecar already exists -> lockBusy (fall back signal).
	res, _, err = c.tryCreate("doc.txt")
	if err != nil {
		t.Fatalf("tryCreate existing: unexpected error %v", err)
	}
	if res != lockBusy {
		t.Fatal("tryCreate on an existing sidecar must report lockBusy")
	}
}

func TestStoreNilBackendIsTransient(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	s := &lockStore{backend: nil, ttl: time.Minute, now: func() time.Time { return now }}
	if _, _, err := s.tryAcquire("doc.txt"); err == nil {
		t.Fatal("nil backend must surface an error (treated as transient), not panic")
	}
}
