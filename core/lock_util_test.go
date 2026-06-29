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

// testLockFS builds a Goofys with file locks enabled whose lock store is wired to
// the given in-memory backend and identity (so manager-level hooks can be tested
// without a live bucket).
func testLockFS(b *fakeLockBackend, session, owner, client string, now *time.Time) *Goofys {
	fs := testGoofys(&cfg.FlagStorage{
		EnableFileLocks: true,
		LockTTL:         30 * time.Minute,
		LockInclude:     cfg.DefaultLockInclude,
	})
	fs.locks.store.backend = b
	fs.locks.store.id = lockIdentity{session: session, owner: owner, client: client}
	fs.locks.store.now = func() time.Time { return *now }
	return fs
}

func lockTestInode(fs *Goofys, name string) (*Inode, *Inode) {
	parent := NewInode(fs, nil, "")
	parent.ToDir()
	return parent, NewInode(fs, parent, name)
}

// TestManagerCheckWriteAcquires verifies that the first write to a free lockable
// file acquires the advisory lock and that another mount then sees it busy.
func TestManagerCheckWriteAcquires(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	fs := testLockFS(b, "sess-a", "ivan", "host-a", &now)
	_, doc := lockTestInode(fs, "report.docx")

	if err := fs.locks.CheckWrite(doc); err != nil {
		t.Fatalf("CheckWrite on free file: want nil, got %v", err)
	}
	if !fs.locks.owns("report.docx") {
		t.Fatal("CheckWrite must acquire the lock on a free file")
	}
	other := newTestStore(b, "sess-b", "petr", "host-b", 30*time.Minute, &now)
	if busy, _, _ := other.busyByOther("report.docx"); !busy {
		t.Fatal("another mount must see the just-acquired lock as busy")
	}
}

// TestManagerCheckWriteForeignDenies verifies a write is denied with EACCES when
// another mount already holds the lock.
func TestManagerCheckWriteForeignDenies(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	foreign := newTestStore(b, "sess-f", "ivan", "host-a", 30*time.Minute, &now)
	mustAcquire(t, foreign, "report.docx")

	fs := testLockFS(b, "sess-mine", "petr", "host-b", &now)
	_, doc := lockTestInode(fs, "report.docx")

	if err := fs.locks.CheckWrite(doc); err != syscall.EACCES {
		t.Fatalf("CheckWrite on foreign-locked file: want EACCES, got %v", err)
	}
	if fs.locks.owns("report.docx") {
		t.Fatal("must not own a lock held by another mount")
	}
}

// TestManagerOnOpenMarksForeignReadOnly verifies a read-only open of a
// foreign-locked file marks the inode read-only (GetAttr strips write bits).
func TestManagerOnOpenMarksForeignReadOnly(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newFakeLockBackend()
	foreign := newTestStore(b, "sess-f", "ivan", "host-a", 30*time.Minute, &now)
	mustAcquire(t, foreign, "report.docx")

	fs := testLockFS(b, "sess-mine", "petr", "host-b", &now)
	_, doc := lockTestInode(fs, "report.docx")
	doc.Attributes.Mode = 0o644

	if err := fs.locks.OnOpen(doc, false); err != nil {
		t.Fatalf("OnOpen(read): unexpected error %v", err)
	}
	if !doc.isLockForeignBusy() {
		t.Fatal("OnOpen on a foreign-locked file must mark the inode foreign-busy")
	}
	if attr := doc.InflateAttributes(); attr.Mode&modeWriteAll != 0 {
		t.Fatalf("foreign-locked inode must have write bits stripped, got mode %o", attr.Mode)
	}
}
