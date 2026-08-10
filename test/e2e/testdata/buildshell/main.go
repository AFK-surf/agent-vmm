package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
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
	if len(os.Args) == 3 && os.Args[1] == "--serve" {
		listener, err := net.Listen("tcp", ":"+os.Args[2])
		if err != nil {
			os.Exit(6)
		}
		for {
			connection, err := listener.Accept()
			if err != nil {
				os.Exit(7)
			}
			go func() { defer connection.Close(); _, _ = io.Copy(connection, connection) }()
		}
	}
	if len(os.Args) == 4 && os.Args[1] == "--write" {
		if err := os.WriteFile(os.Args[2], []byte(os.Args[3]+"\n"), 0o644); err != nil {
			os.Exit(8)
		}
		fmt.Println(os.Args[3])
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "--cpu" {
		deadline := time.Now().Add(500 * time.Millisecond)
		var iterations uint64
		for time.Now().Before(deadline) {
			iterations++
		}
		if iterations == 0 {
			os.Exit(9)
		}
		fmt.Println("cpu-ok")
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
