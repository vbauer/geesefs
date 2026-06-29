// Lock store: the pure S3 protocol for advisory sidecar locks.
//
// lockStore has no knowledge of FUSE, inodes or file handles, so it is
// unit-testable against an in-memory backend (see lock_store_test.go). It is
// stateless with respect to local hold tracking: methods return results/etags
// and let the caller (FileLockManager) keep the in-mount state. The cross-mount
// correctness guarantee comes entirely from S3 conditional writes
// (If-None-Match:* on create, If-Match:<etag> on update) — the CAS, not any
// local map, is what makes "first writer wins".
//
// Round-trips: the sidecar is a tiny JSON object and GetBlob already returns the
// ETag, so the protocol reads it with a single GET (no preceding HEAD). A 404
// from the GET means "no sidecar"; a 200 yields both the record and the etag
// needed for the next If-Match update.

package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// lockBackend is the narrow subset of StorageBackend the lock protocol needs.
// StorageBackend satisfies it; tests provide an in-memory fake.
type lockBackend interface {
	GetBlob(*GetBlobInput) (*GetBlobOutput, error)
	PutBlob(*PutBlobInput) (*PutBlobOutput, error)
}

// lockIdentity is who this mount is for lock ownership/reclaim decisions.
type lockIdentity struct {
	session string // unique per geesefs process (UUID)
	owner   string // display name (also used for same-host reclaim)
	client  string // hostname (same-host reclaim)
}

type lockStore struct {
	backend lockBackend // the mount's storage backend; nil in pure-helper mode (locks off / tests)
	id      lockIdentity
	ttl     time.Duration
	now     func() time.Time // injectable clock (tests)
}

var (
	errNoLockBackend = fmt.Errorf("lock backend unavailable")
	// errBadRecord marks an unparseable/unsupported sidecar — treated as stale
	// (reclaimable), distinct from a transient I/O error.
	errBadRecord = errors.New("invalid lock sidecar")
)

func (s *lockStore) cloud() (lockBackend, error) {
	if s.backend == nil {
		return nil, errNoLockBackend
	}
	return s.backend, nil
}

// busyByOther reports whether another holder currently owns the lock on dataKey.
// A missing/expired/released/corrupt sidecar, our own session, or a same-host
// reclaimable record are all "not busy".
func (s *lockStore) busyByOther(dataKey string) (busy bool, holder string, err error) {
	rec, _, err := s.readRecord(lockKey(dataKey))
	if err != nil {
		if mapAwsError(err) == syscall.ENOENT || errors.Is(err, errBadRecord) {
			return false, "", nil // no sidecar, or corrupt -> not busy
		}
		return false, "", err // transient
	}
	if lockRecordReclaimable(rec, s.id, s.expired) {
		return false, "", nil // free, ours, expired, or same-host reclaimable
	}
	holder = rec.Owner
	if holder == "" {
		holder = rec.Client
	}
	return true, holder, nil
}

// tryAcquire attempts to take the lock for dataKey. On success it returns the
// new sidecar etag (needed for later heartbeat/release CAS).
func (s *lockStore) tryAcquire(dataKey string) (lockAcquireResult, string, error) {
	lk := lockKey(dataKey)

	rec, etag, err := s.readRecord(lk)
	if err != nil {
		if mapAwsError(err) == syscall.ENOENT {
			return s.create(lk) // no sidecar yet
		}
		if errors.Is(err, errBadRecord) {
			s3Log.Warnf("Invalid lock sidecar %v, treating as stale: %v", lk, err)
			return s.put(lk, etag, true) // reclaim with known etag
		}
		return lockBusy, "", err // transient
	}
	if !rec.Held || s.expired(rec) {
		return s.put(lk, etag, true) // stale reclaim
	}
	if rec.Session == s.id.session {
		return lockAcquired, etag, nil // re-entry
	}
	if lockRecordReclaimable(rec, s.id, s.expired) {
		return s.put(lk, etag, true) // same-host remount
	}
	return lockBusy, "", nil
}

// tryCreate is the optimistic fast path used when the caller already believes
// the lock is free (negative cache): it blind-creates the sidecar with
// If-None-Match:* without a preceding GET. It returns (lockBusy, "", nil) when
// the sidecar already exists, signalling the caller to fall back to tryAcquire.
func (s *lockStore) tryCreate(dataKey string) (lockAcquireResult, string, error) {
	return s.create(lockKey(dataKey))
}

// create writes the initial held sidecar with If-None-Match:* (first writer wins).
func (s *lockStore) create(lk string) (lockAcquireResult, string, error) {
	cloud, err := s.cloud()
	if err != nil {
		return lockBusy, "", err
	}
	body, err := s.newBody()
	if err != nil {
		return lockBusy, "", err
	}
	ct := "application/json"
	resp, err := cloud.PutBlob(&PutBlobInput{
		Key:         lk,
		Body:        bytes.NewReader(body),
		Size:        PUInt64(uint64(len(body))),
		ContentType: &ct,
		IfNoneMatch: PString("*"),
		Tags:        map[string]string{"geesefs-lock": "true"},
	})
	if err != nil {
		if isPreconditionFailed(err) {
			// Sidecar already exists (concurrent creator or a leftover record):
			// re-evaluate via the full path.
			return s.tryAcquire(dataKeyFromLockPath(lk))
		}
		return lockBusy, "", err
	}
	return lockAcquired, derefEtag(resp.ETag), nil
}

// put writes a held or released record, using If-Match CAS when etag is known.
// A 412 means we lost the race: for held that is lockBusy, for release that is
// a benign no-op (someone already replaced the record).
func (s *lockStore) put(lk, etag string, held bool) (lockAcquireResult, string, error) {
	cloud, err := s.cloud()
	if err != nil {
		return lockBusy, "", err
	}
	var body []byte
	if held {
		body, err = s.newBody()
	} else {
		body, err = json.Marshal(lockRecord{Version: 1, Held: false})
	}
	if err != nil {
		return lockBusy, "", err
	}
	ct := "application/json"
	in := &PutBlobInput{
		Key:         lk,
		Body:        bytes.NewReader(body),
		Size:        PUInt64(uint64(len(body))),
		ContentType: &ct,
		Tags:        map[string]string{"geesefs-lock": "true"},
	}
	if etag != "" {
		in.IfMatch = &etag
	}
	resp, err := cloud.PutBlob(in)
	if err != nil {
		if isPreconditionFailed(err) {
			if held {
				return lockBusy, "", nil
			}
			return lockAcquired, "", nil
		}
		return lockBusy, "", err
	}
	return lockAcquired, derefEtag(resp.ETag), nil
}

// renew refreshes expires_at on a lock we believe we hold. Result semantics for
// the caller:
//   - (lockAcquired, etag, nil): renewed, store the new etag.
//   - (lockBusy, "", nil):       confirmed loss (reclaimed/released or 412) -> downgrade.
//   - (_, _, err != nil):        transient error -> keep the hold, retry next tick.
func (s *lockStore) renew(lk, etag string) (lockAcquireResult, string, error) {
	rec, cur, err := s.readRecord(lk)
	if err != nil {
		if mapAwsError(err) == syscall.ENOENT {
			return lockBusy, "", nil // sidecar gone -> lost
		}
		return lockBusy, "", err // transient (incl. corrupt) -> keep, retry
	}
	if cur == "" {
		cur = etag
	}
	if rec.Session != s.id.session || !rec.Held {
		return lockBusy, "", nil // reclaimed or released by someone else
	}
	return s.put(lk, cur, true)
}

// release writes held:false if we still own the sidecar. We never DELETE so the
// protocol works identically on versioned buckets (a delete marker would break
// the next If-None-Match:* create).
func (s *lockStore) release(lk string) error {
	rec, etag, err := s.readRecord(lk)
	if err != nil {
		if mapAwsError(err) == syscall.ENOENT {
			return nil // already gone
		}
		return err
	}
	if rec.Session != s.id.session {
		return nil // not ours; leave it
	}
	_, _, err = s.put(lk, etag, false)
	return err
}

// readRecord fetches and parses the sidecar with a single GET, also returning
// its etag. A 404 surfaces as an ENOENT error (caller maps it); a parse/version
// problem surfaces as errBadRecord (with the etag still returned, so the caller
// can reclaim a corrupt record via If-Match).
func (s *lockStore) readRecord(lk string) (*lockRecord, string, error) {
	cloud, err := s.cloud()
	if err != nil {
		return nil, "", err
	}
	resp, err := cloud.GetBlob(&GetBlobInput{Key: lk})
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	etag := derefEtag(resp.ETag)
	var rec lockRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, etag, fmt.Errorf("%w: %v", errBadRecord, err)
	}
	if rec.Version != 1 {
		return nil, etag, fmt.Errorf("%w: unsupported version %d", errBadRecord, rec.Version)
	}
	return &rec, etag, nil
}

func (s *lockStore) newBody() ([]byte, error) {
	expires := s.now().UTC().Add(s.ttl).Format(time.RFC3339)
	return json.Marshal(lockRecord{
		Version:   1,
		Held:      true,
		Owner:     s.id.owner,
		Session:   s.id.session,
		Client:    s.id.client,
		ExpiresAt: expires,
	})
}

func (s *lockStore) expired(rec *lockRecord) bool {
	if rec.ExpiresAt == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, rec.ExpiresAt)
	if err != nil {
		return true
	}
	return s.now().UTC().After(t)
}

func derefEtag(e *string) string {
	if e != nil {
		return *e
	}
	return ""
}

func isPreconditionFailed(err error) bool {
	if reqErr, ok := err.(awserr.RequestFailure); ok {
		return reqErr.StatusCode() == http.StatusPreconditionFailed
	}
	return false
}

// dataKeyFromLockPath inverts lockKey: ".../.<base>.geesefs-lock" -> ".../<base>".
func dataKeyFromLockPath(lk string) string {
	dir, base := pathSplitLock(lk)
	if !isLockSidecarName(base) {
		return lk
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(base, "."), lockSidecarSuffix)
	if dir != "" {
		return dir + "/" + inner
	}
	return inner
}

func pathSplitLock(key string) (string, string) {
	i := len(key) - 1
	for i >= 0 && key[i] != '/' {
		i--
	}
	if i < 0 {
		return "", key
	}
	return key[:i], key[i+1:]
}
