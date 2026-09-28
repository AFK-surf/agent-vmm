package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	if err := exerciseFiles(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("file cache workload completed")
}

func exerciseFiles() error {
	// Each mount has a separate gofer cache. Close application descriptors
	// promptly while keeping enough cached files to exceed the old host limit.
	directories := []string{"/root-files", "/first/files", "/second/files"}
	for _, directory := range directories {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
	}
	for round := 0; round < 2; round++ {
		for _, directory := range directories {
			for file := 0; file < 800; file++ {
				path := filepath.Join(directory, fmt.Sprintf("f%04d", file))
				if err := os.WriteFile(path, []byte("retained file"), 0o600); err != nil {
					return err
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if string(data) != "retained file" {
					return fmt.Errorf("unexpected file content at %s", path)
				}
			}
		}
	}
	for _, directory := range directories {
		if err := os.RemoveAll(directory); err != nil {
			return err
		}
	}
	return nil
}
