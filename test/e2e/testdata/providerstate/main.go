package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	if len(os.Args) != 2 {
		fatal("one operation is required")
	}
	switch os.Args[1] {
	case "idle":
		stopped := make(chan os.Signal, 1)
		signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
		<-stopped
	case "write-codex", "write-pi":
		provider := os.Args[1][len("write-"):]
		write(provider)
	case "read-codex", "read-pi":
		provider := os.Args[1][len("read-"):]
		read(provider)
	default:
		fatal("unsupported operation")
	}
}

func credentialPath(provider string) string {
	home := os.Getenv("HOME")
	if home == "" {
		fatal("HOME is required")
	}
	return filepath.Join(home, "."+provider, "auth.json")
}

func write(provider string) {
	path := credentialPath(provider)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		fatal("create provider directory")
	}
	credential := make([]byte, 32)
	if _, err := rand.Read(credential); err != nil {
		fatal("generate provider state")
	}
	if err := os.WriteFile(path, credential, 0o600); err != nil {
		fatal("write provider state")
	}
	fmt.Println("authenticated")
}

func read(provider string) {
	info, err := os.Stat(credentialPath(provider))
	if err != nil || info.Size() != 32 || info.Mode().Perm() != 0o600 {
		fatal("provider state is unavailable")
	}
	fmt.Println("authenticated")
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
