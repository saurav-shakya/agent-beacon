package sessionsync

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Two sweeps of one collector can save its cursor at the same moment, as a scheduled sync and the
// backfill an install or connect runs do. Each save must land whole: one that fails, or a file left
// holding parts of both, stops every later sweep at "read ... collector state".
func TestStateFileConcurrentSavesLeaveAWholeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.json")
	const writers = 8
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for w := 0; w < writers; w++ {
			state := testFile.New()
			// Big enough that a write is not one syscall, and different per writer.
			for i := 0; i < 2000+w*500; i++ {
				n := int64(i)
				state.Files[fmt.Sprintf("file-%d-%d", w, i)] = &n
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- testFile.Save(path, state)
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: concurrent save failed: %v", round, err)
			}
		}
		if _, err := testFile.Load(path); err != nil {
			t.Fatalf("round %d: concurrent saves left a file that does not load: %v", round, err)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("directory holds %v, want only the state file", names)
	}
}
