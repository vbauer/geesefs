package core

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jacobsa/fuse/fuseops"
)

// Advisory file locking: one S3 sidecar per lockable data object.
//
//   - lock_store.go : pure S3 protocol (CAS, TTL) — no FUSE, unit-tested.
//   - lock.go       : FileLockManager — maps FUSE events (Open/Write/Create/
//                     Rename/Close) to the store and tracks per-mount state.
//   - lock_rules.go : include/exclude globs.
//   - lock_util.go  : sidecar naming / lock-subject resolution.
//
// State model (single source of truth):
//   - "we own key K"        == K present in FileLockManager.locks (guarded by mu).
//   - "another mount owns K" == inode.lockForeignBusy (ephemeral; re-derived from S3).
//   - inode.lockForeignBusy is a DERIVED cache of the above, so GetAttr can strip
//     write bits lock-free. It is only ever mutated through markOwned / markForeign /
//     markFree, which keep it in sync with the map.
//   - File handles carry NO lock state.

type lockRecord struct {
	Version   int    `json:"version"`
	Held      bool   `json:"held"`
	Owner     string `json:"owner,omitempty"`
	Session   string `json:"session,omitempty"`
	Client    string `json:"client,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type lockAcquireResult int

const (
	lockAcquired lockAcquireResult = iota
	lockBusy
)

// lockNegFreeTTL bounds the per-inode "lock was free" negative cache: within
// this window repeated opens skip the detection GET (and acquire optimistically
// creates the sidecar). Short, so a foreign lock taken meanwhile is noticed soon.
const lockNegFreeTTL = 2 * time.Second

// fileLock is the per-mount state for a single locked data object we own.
// Guarded by FileLockManager.mu.
type fileLock struct {
	lockKey string
	inode   *Inode // subject inode, for mirroring flags / downgrading on loss
	etag    string // current sidecar etag (for heartbeat/release CAS)
}

// FileLockManager holds path rules (include/exclude) and, when enabled,
// coordinates advisory locks stored as S3 sidecar objects via lockStore. It does
// not hold a *Goofys back-pointer: it reaches the backend through lockStore and
// the include/exclude rules through m.rules; per-op it gets the fs via the
// inode/parent passed into each hook.
type FileLockManager struct {
	rules   lockRules
	enabled bool
	store   *lockStore

	mu        sync.Mutex
	locks     map[string]*fileLock // dataKey -> state for locks WE own
	releaseWg sync.WaitGroup
}

// initFileLockManager fills the manager in place (pointer receiver so the
// embedded sync.Mutex is never copied). The store is always created so its pure
// helpers are usable even when locking is disabled; FUSE hooks early-return on
// !enabled. Returns an error when locking is enabled on a backend that cannot
// support it (see lockUnsupportedBackend).
func (m *FileLockManager) initFileLockManager(fs *Goofys) error {
	m.rules = *newLockRules(fs.flags)
	m.enabled = fs.flags.EnableFileLocks
	m.locks = make(map[string]*fileLock)
	// The root cloud is set before initFileLockManager runs (newGoofys), so we can
	// capture the backend directly rather than resolving it lazily on every call.
	cloud, _ := fs.rootCloud()
	m.store = &lockStore{
		backend: cloud,
		ttl:     fs.flags.LockTTL,
		now:     time.Now,
	}
	if !m.enabled {
		return nil
	}
	if cloud != nil {
		if name := cloud.Capabilities().Name; lockUnsupportedBackend(name) {
			return fmt.Errorf("--enable-file-locks is not supported on the %q backend: "+
				"advisory locks need conditional writes (CAS), which it does not provide", name)
		}
	}
	host, _ := os.Hostname()
	m.store.id = lockIdentity{
		session: uuid.New().String(),
		owner:   defaultLockOwner(fs.flags.LockOwner),
		client:  host,
	}
	lockLog.Infof("file locks enabled owner=%q session=%v client=%q include=%q exclude=%q",
		m.store.id.owner, m.store.id.session, m.store.id.client, fs.flags.LockInclude, fs.flags.LockExclude)
	return nil
}

// lockUnsupportedBackend reports whether a backend (by Capabilities().Name) cannot
// back advisory locks. ADL Gen2 ("adl2") writes via a non-atomic create+append+flush
// and currently ignores If-Match/If-None-Match, so "first writer wins" cannot hold;
// we refuse explicitly instead of silently providing no protection.
func lockUnsupportedBackend(name string) bool {
	return name == "adl2"
}

func (m *FileLockManager) excluded(dataKey string) bool { return m.rules.excluded(dataKey) }
func (m *FileLockManager) included(dataKey string) bool { return m.rules.included(dataKey) }

// lockExpired is kept for tests and callers that already hold a record.
func (m *FileLockManager) lockExpired(rec *lockRecord) bool { return m.store.expired(rec) }

// --- state transitions (the only places inode flags are mutated) ---

// markOwned records that we hold dataKey and mirrors it to the inode flags.
// A non-empty etag updates the stored CAS etag; "" leaves it (re-entry refresh).
func (m *FileLockManager) markOwned(dataKey string, inode *Inode, etag string) {
	m.mu.Lock()
	fl := m.locks[dataKey]
	if fl == nil {
		fl = &fileLock{lockKey: lockKey(dataKey)}
		m.locks[dataKey] = fl
	}
	if inode != nil {
		fl.inode = inode
	}
	if etag != "" {
		fl.etag = etag
	}
	target := fl.inode
	m.mu.Unlock()
	if target != nil {
		atomic.StoreInt32(&target.lockForeignBusy, lockFlagOff)
		atomic.StoreInt64(&target.lockFreeAt, 0) // we own it now; invalidate negative cache
	}
}

// markForeign records that another mount holds dataKey: drop any ownership we
// thought we had and flag the inode read-only (writes -> EACCES, reads OK).
func (m *FileLockManager) markForeign(dataKey string, inode *Inode) {
	target := m.dropAndResolveInode(dataKey, inode)
	if target != nil {
		atomic.StoreInt32(&target.lockForeignBusy, lockFlagOn)
		atomic.StoreInt64(&target.lockFreeAt, 0) // not free; invalidate negative cache
	}
}

// markFree records that nobody holds dataKey (as far as we know) and refreshes
// the negative cache so nearby opens can skip the detection GET.
func (m *FileLockManager) markFree(dataKey string, inode *Inode) {
	target := m.dropAndResolveInode(dataKey, inode)
	if target != nil {
		atomic.StoreInt32(&target.lockForeignBusy, lockFlagOff)
		atomic.StoreInt64(&target.lockFreeAt, m.store.now().UnixNano())
	}
}

// recentlyFree reports whether this inode's lock was confirmed free within the
// negative-cache window.
func (m *FileLockManager) recentlyFree(inode *Inode) bool {
	if inode == nil {
		return false
	}
	at := atomic.LoadInt64(&inode.lockFreeAt)
	return at != 0 && m.store.now().UnixNano()-at < int64(lockNegFreeTTL)
}

// dropAndResolveInode removes any owned entry for dataKey and returns the inode
// whose flags should be updated (prefers the stored subject inode).
func (m *FileLockManager) dropAndResolveInode(dataKey string, inode *Inode) *Inode {
	target := lockSubjectInode(inode)
	if target == nil {
		target = inode
	}
	m.mu.Lock()
	if fl := m.locks[dataKey]; fl != nil {
		if fl.inode != nil {
			target = fl.inode
		}
		delete(m.locks, dataKey)
	}
	m.mu.Unlock()
	return target
}

func (m *FileLockManager) owns(dataKey string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.locks[dataKey] != nil
}

// acquire takes the lock for dataKey (or confirms re-entry) and records the
// result. owned=true means we hold it afterwards; transient=true means an S3
// error left the outcome unknown (caller should not latch read-only).
func (m *FileLockManager) acquire(dataKey string, inode *Inode) (owned bool, transient bool) {
	if m.owns(dataKey) {
		m.markOwned(dataKey, inode, "") // refresh inode flags on re-open
		return true, false
	}
	if m.recentlyFree(inode) {
		// Optimistic fast path: skip the detection GET and create the sidecar
		// directly. On 412 (it already exists) fall through to the full path.
		if result, etag, err := m.store.tryCreate(dataKey); err == nil && result == lockAcquired {
			m.markOwned(dataKey, inode, etag)
			lockLog.Debugf("acquire %v: ACQUIRED (optimistic create)", dataKey)
			return true, false
		}
	}
	result, etag, err := m.store.tryAcquire(dataKey)
	if err != nil {
		s3Log.Warnf("lock acquire %v: %v", dataKey, err)
		return false, true
	}
	if result == lockBusy {
		m.markForeign(dataKey, inode)
		return false, false
	}
	m.markOwned(dataKey, inode, etag)
	lockLog.Debugf("acquire %v: ACQUIRED session=%v", dataKey, m.store.id.session)
	return true, false
}

// --- lifecycle ---

// Start runs a single background heartbeat for all locks held by this mount.
// shutdownCh is the mount's shutdown signal (closed on Goofys.Shutdown); the
// manager takes it as an argument rather than holding a *Goofys back-pointer.
func (m *FileLockManager) Start(shutdownCh <-chan struct{}) {
	if !m.enabled {
		return
	}
	interval := m.store.ttl / 3
	if interval < time.Minute {
		interval = time.Minute
	}
	go m.heartbeatLoop(interval, shutdownCh)
}

func (m *FileLockManager) ReleaseAll() {
	if !m.enabled {
		return
	}
	m.mu.Lock()
	keys := make([]string, 0, len(m.locks))
	for dataKey := range m.locks {
		keys = append(keys, dataKey)
	}
	m.mu.Unlock()
	for _, dataKey := range keys {
		if err := m.store.release(lockKey(dataKey)); err != nil {
			lockLog.Debugf("release %v: %v", dataKey, err)
		}
	}
	m.releaseWg.Wait()
}

// --- FUSE hooks ---

func (m *FileLockManager) OnOpen(inode *Inode, writeIntent bool) error {
	if !m.enabled || inode.isDir() {
		return nil
	}
	dataKey := lockSubjectDataKey(inode)
	if dataKey == "" {
		return nil
	}
	lockLog.Debugf("OnOpen %v writeIntent=%v", dataKey, writeIntent)

	if writeIntent {
		// Acquire synchronously: the writer must publish the sidecar before a
		// second user can observe "busy". A transient error leaves us unlocked;
		// the first write retries via CheckWrite.
		m.acquire(dataKey, inode)
		return nil
	}

	// Read-only open: only detect foreign locks, to surface read-only attrs.
	if m.owns(dataKey) {
		m.markOwned(dataKey, inode, "")
		return nil
	}
	if m.recentlyFree(inode) {
		// Known-free recently: skip the detection GET.
		m.markFree(dataKey, inode)
		return nil
	}
	busy, holder, err := m.store.busyByOther(dataKey)
	if err != nil {
		// Transient error: don't latch read-only; a later op re-checks.
		s3Log.Warnf("lock busy check on open %v: %v", dataKey, err)
		m.markFree(dataKey, inode)
		return nil
	}
	if busy {
		lockLog.Debugf("OnOpen %v: read-only, busy by %q", dataKey, holder)
		m.markForeign(dataKey, inode)
	} else {
		m.markFree(dataKey, inode)
	}
	return nil
}

// CheckWrite is called on the write path (under inode.mu). It denies the write
// when another mount holds the lock, and lazily acquires when free.
func (m *FileLockManager) CheckWrite(inode *Inode) error {
	if !m.enabled || inode.isDir() {
		return nil
	}
	dataKey := lockSubjectDataKey(inode)
	if dataKey == "" {
		return nil
	}
	if m.owns(dataKey) {
		return nil
	}
	owned, transient := m.acquire(dataKey, inode)
	if owned {
		lockLog.Debugf("CheckWrite %v: acquired on write", dataKey)
		return nil
	}
	if transient {
		// Unknown state: deny this write but don't latch — next write retries.
		lockLog.Debugf("CheckWrite %v: EACCES (transient)", dataKey)
		return syscall.EACCES
	}
	lockLog.Debugf("CheckWrite %v: EACCES (busy)", dataKey)
	return syscall.EACCES
}

func (m *FileLockManager) CheckCreate(parent *Inode, name string) error {
	if !m.enabled {
		return nil
	}
	if isLockSidecarName(name) {
		return syscall.EACCES
	}
	return m.checkForeignLock(lockSubjectForChild(parent, name))
}

func (m *FileLockManager) CheckMkDir(parent *Inode, name string) error {
	if !m.enabled {
		return nil
	}
	return m.checkForeignLock(lockSubjectForChild(parent, name))
}

// CheckRename guards a rename: the destination must not be locked by another
// client (consistent with CheckCreate), and renaming onto a hidden sidecar is denied.
func (m *FileLockManager) CheckRename(newParent *Inode, newName string) error {
	if !m.enabled {
		return nil
	}
	if isLockSidecarName(newName) {
		return syscall.EACCES
	}
	return m.checkForeignLock(lockSubjectForChild(newParent, newName))
}

// OnRename runs after a successful rename. If we held the lock on the old key,
// release its sidecar so it does not linger as a zombie (the heartbeat would keep
// renewing it forever). The same inode continues under the new key and re-acquires
// lazily on its next write via CheckWrite.
func (m *FileLockManager) OnRename(oldParent *Inode, oldName string) {
	if !m.enabled {
		return
	}
	oldKey := lockSubjectForChild(oldParent, oldName)
	if oldKey == "" {
		return
	}
	if !m.owns(oldKey) {
		return
	}
	m.markFree(oldKey, nil) // clears map + inode flags (uses stored inode)
	lockLog.Debugf("OnRename %v: releasing sidecar on old key", oldKey)
	m.releaseKeyAsync(oldKey)
}

func (m *FileLockManager) checkForeignLock(dataKey string) error {
	if dataKey == "" {
		return nil
	}
	if m.owns(dataKey) {
		return nil
	}
	busy, holder, err := m.store.busyByOther(dataKey)
	if err != nil {
		s3Log.Warnf("lock busy check on %v: %v", dataKey, err)
		return syscall.EACCES
	}
	if busy {
		lockLog.Debugf("foreign lock on %v (holder %q): EACCES", dataKey, holder)
		return syscall.EACCES
	}
	return nil
}

// OnInodeClosed releases the sidecar when the last handle on a lock subject closes.
func (m *FileLockManager) OnInodeClosed(inode *Inode) {
	if !m.enabled {
		return
	}
	_, ownKey := inode.cloud()
	if !shouldLockDataKey(inode.fs, ownKey) {
		return
	}
	if !m.owns(ownKey) {
		return
	}
	if atomic.LoadInt32(&inode.fileHandles) > 0 {
		return
	}
	lockLog.Debugf("OnInodeClosed %v: releasing sidecar", ownKey)
	m.finalizeRelease(inode, ownKey)
}

func (m *FileLockManager) finalizeRelease(inode *Inode, dataKey string) {
	if atomic.LoadInt32(&inode.fileHandles) > 0 {
		lockLog.Debugf("finalizeRelease %v: skipped, handles reopened", dataKey)
		return
	}
	if !m.owns(dataKey) {
		return
	}
	m.markFree(dataKey, inode)
	m.releaseKeyAsync(dataKey)
}

func (m *FileLockManager) releaseKeyAsync(dataKey string) {
	lk := lockKey(dataKey)
	m.releaseWg.Add(1)
	go func() {
		defer m.releaseWg.Done()
		if err := m.store.release(lk); err != nil {
			lockLog.Debugf("release %v: %v", dataKey, err)
		}
	}()
}

// --- heartbeat ---

func (m *FileLockManager) heartbeatLoop(interval time.Duration, shutdownCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-shutdownCh:
			return
		case <-ticker.C:
			m.heartbeat()
		}
	}
}

func (m *FileLockManager) heartbeat() {
	type job struct{ dataKey, lockKey, etag string }
	m.mu.Lock()
	jobs := make([]job, 0, len(m.locks))
	for dataKey, fl := range m.locks {
		jobs = append(jobs, job{dataKey, fl.lockKey, fl.etag})
	}
	m.mu.Unlock()

	for _, j := range jobs {
		result, etag, err := m.store.renew(j.lockKey, j.etag)
		if err != nil {
			continue // transient: keep the hold, retry next tick
		}
		if result == lockBusy {
			// Confirmed loss: our sidecar was reclaimed/released. Stop allowing
			// writes that would overwrite the new holder.
			s3Log.Warnf("Lock heartbeat lost for %v", j.dataKey)
			m.markForeign(j.dataKey, nil)
			continue
		}
		m.mu.Lock()
		if fl := m.locks[j.dataKey]; fl != nil {
			fl.etag = etag
		}
		m.mu.Unlock()
	}
}

// --- Goofys glue ---

func (fs *Goofys) rollbackFileHandleOpen(handle fuseops.HandleID, fh *FileHandle) {
	fs.mu.Lock()
	delete(fs.fileHandles, handle)
	fs.mu.Unlock()
	fh.Release()
}

func (fs *Goofys) rootCloud() (StorageBackend, string) {
	fs.mu.RLock()
	root := fs.inodes[fuseops.RootInodeID]
	fs.mu.RUnlock()
	if root == nil {
		return nil, ""
	}
	return root.cloud()
}
