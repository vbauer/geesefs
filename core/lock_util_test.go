package core

import (
	"syscall"
	"testing"
	"time"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestLockKey(t *testing.T) {
	if got := lockKey("finance/Q1.xlsx"); got != "finance/.Q1.xlsx.geesefs-lock" {
		t.Fatalf("lockKey finance: got %q", got)
	}
	if got := lockKey("file.txt"); got != ".file.txt.geesefs-lock" {
		t.Fatalf("lockKey root file: got %q", got)
	}
}

func TestIsLockSidecarName(t *testing.T) {
	if !isLockSidecarName(".Q1.xlsx.geesefs-lock") {
		t.Fatal("expected sidecar name")
	}
	if isLockSidecarName("Q1.xlsx.geesefs-lock") {
		t.Fatal("suffix without dot prefix must not match")
	}
}

func TestShouldLockDataKey(t *testing.T) {
	fs := testGoofys(nil)
	if !shouldLockDataKey(fs, "geesefs-test.docx") {
		t.Fatal("main document should be locked")
	}
	if shouldLockDataKey(fs, "~$geesefs-test.docx") {
		t.Fatal("editor lock marker should be excluded")
	}
	if shouldLockDataKey(fs, "geesefs-test.docx.sb-15d02470-gHvegY/.~WRD0000") {
		t.Fatal("sandbox temp should be excluded")
	}
	if shouldLockDataKey(fs, "geesefs-test.docx.sb-15d02470-gHvegY/..~WRD0002") {
		t.Fatal("sandbox temp with extra dot should be excluded")
	}
	if shouldLockDataKey(fs, "geesefs-test.docx.sb-15d02470-ECklaz/.~WRL0001") {
		t.Fatal("sandbox write-lock temp should be excluded")
	}
	if !shouldLockDataKey(fs, "finance/Q1.xlsx") {
		t.Fatal("nested document should be locked")
	}
}

func TestShouldLockDataKeyInclude(t *testing.T) {
	fs := testGoofys(&cfg.FlagStorage{LockInclude: "*.docx"})
	if !shouldLockDataKey(fs, "report.docx") {
		t.Fatal("docx should match include")
	}
	if shouldLockDataKey(fs, "report.xlsx") {
		t.Fatal("xlsx should not match include")
	}
}

func TestLockRecordReclaimable(t *testing.T) {
	expired := func(rec *lockRecord) bool { return false }
	held := &lockRecord{
		Held:    true,
		Session: "old-session",
		Owner:   "vbauer",
		Client:  "machine-a",
	}
	if !lockRecordReclaimable(held, lockIdentity{session: "new-session", owner: "vbauer", client: "machine-a"}, expired) {
		t.Fatal("same host remount should reclaim")
	}
	if lockRecordReclaimable(held, lockIdentity{session: "new-session", owner: "vbauer", client: "machine-b"}, expired) {
		t.Fatal("same owner different host must not reclaim")
	}
	if !lockRecordReclaimable(held, lockIdentity{session: "old-session", owner: "vbauer", client: "machine-b"}, expired) {
		t.Fatal("same session should reclaim")
	}
}

func TestOpenWantsWrite(t *testing.T) {
	if openWantsWrite(0) {
		t.Fatal("O_RDONLY should not want write")
	}
	if !openWantsWrite(1) || !openWantsWrite(2) {
		t.Fatal("O_WRONLY/O_RDWR should want write")
	}
}

func TestLockSubjectInode(t *testing.T) {
	fs := testGoofys(nil)
	parent := NewInode(fs, nil, "")
	parent.ToDir()
	doc := NewInode(fs, parent, "geesefs-test.docx")

	if got := lockSubjectInode(doc); got != doc {
		t.Fatal("lock subject inode should be itself")
	}
	if got := lockSubjectDataKey(doc); got != "geesefs-test.docx" {
		t.Fatalf("lock subject key: got %q", got)
	}
}

func TestLockRecordStale(t *testing.T) {
	fs := testGoofys(&cfg.FlagStorage{EnableFileLocks: true, LockTTL: 30 * time.Minute})
	rec := &lockRecord{ExpiresAt: "2000-01-01T00:00:00Z", Held: true}
	if !fs.locks.store.expired(rec) {
		t.Fatal("expected expired lock")
	}
}

// TestCheckMutateUnlinkForeign verifies that mutations (truncate/fallocate/chmod)
// and unlink are denied with EACCES while another mount holds the advisory lock,
// and allowed on a free file.
func TestCheckMutateUnlinkForeign(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()

	// Another mount takes the lock on report.docx.
	other := newTestStore(b, "sess-other", "ivan", "host-a", 30*time.Minute, &now)
	mustAcquire(t, other, "report.docx")

	// Our mount: locks enabled, store wired to the same in-memory backend.
	fs := testGoofys(&cfg.FlagStorage{
		EnableFileLocks: true,
		LockTTL:         30 * time.Minute,
		LockInclude:     cfg.DefaultLockInclude,
	})
	fs.locks.store.backend = b
	fs.locks.store.id = lockIdentity{session: "sess-mine", owner: "petr", client: "host-b"}
	fs.locks.store.now = func() time.Time { return now }

	parent := NewInode(fs, nil, "")
	parent.ToDir()
	doc := NewInode(fs, parent, "report.docx")

	if err := fs.locks.CheckMutate(doc); err != syscall.EACCES {
		t.Fatalf("CheckMutate on foreign-locked file: want EACCES, got %v", err)
	}
	if err := fs.locks.CheckUnlink(parent, "report.docx"); err != syscall.EACCES {
		t.Fatalf("CheckUnlink on foreign-locked file: want EACCES, got %v", err)
	}

	// A free file is unaffected.
	free := NewInode(fs, parent, "free.docx")
	if err := fs.locks.CheckMutate(free); err != nil {
		t.Fatalf("CheckMutate on free file: want nil, got %v", err)
	}
	if err := fs.locks.CheckUnlink(parent, "free.docx"); err != nil {
		t.Fatalf("CheckUnlink on free file: want nil, got %v", err)
	}
}
