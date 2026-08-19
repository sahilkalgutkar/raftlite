package node

import "path/filepath"

// filepathGlob lists the snapshot files in a directory.
func filepathGlob(dir string) ([]string, error) {
	return filepath.Glob(filepath.Join(dir, "*.snap"))
}
