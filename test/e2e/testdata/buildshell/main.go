package main

import (
	"bytes"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "-c" {
		secret, err := os.ReadFile("/run/secrets/token")
		if err != nil || len(secret) == 0 {
			os.Exit(2)
		}
		if os.Getenv("MESSAGE") == "build-cancel" {
			time.Sleep(5 * time.Minute)
		}
		if err := os.WriteFile("/result", []byte(os.Getenv("MESSAGE")+"\n"), 0o644); err != nil {
			os.Exit(3)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "--hold" {
		time.Sleep(5 * time.Minute)
		return
	}
	result, err := os.ReadFile("/result")
	if err != nil {
		os.Exit(4)
	}
	if len(os.Args) == 3 && os.Args[1] == "--assert-absent" && bytes.Contains(result, []byte(os.Args[2])) {
		os.Exit(5)
	}
	fmt.Print(string(result))
}
