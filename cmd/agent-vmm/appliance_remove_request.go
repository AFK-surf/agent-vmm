//go:build darwin || linux

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/protobuf/proto"
)

type applianceRemoveCommand struct {
	Timeout time.Duration                     `json:"timeout_nanos"`
	Request *appliancev1.ExecuteRemoveRequest `json:"request"`
}

func freezeApplianceRemove(socket string, request *appliancev1.ExecuteRemoveRequest, timeout time.Duration) (*appliancev1.ExecuteRemoveRequest, error) {
	request = proto.Clone(request).(*appliancev1.ExecuteRemoveRequest)
	slices.Sort(request.ConfirmedResourceIds)
	request.ConfirmedResourceIds = slices.Compact(request.ConfirmedResourceIds)
	canonicalSocket, err := filepath.EvalSymlinks(socket)
	if err != nil {
		return nil, err
	}
	canonicalSocket, err = filepath.Abs(canonicalSocket)
	if err != nil {
		return nil, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	directory := filepath.Join(platformDataDirectory(home), "appliance-requests")
	key := sha256.Sum256([]byte(canonicalSocket + "\x00" + request.GetRequestId()))
	path := filepath.Join(directory, "remove-"+hex.EncodeToString(key[:])+".json")
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(directory, 0700); err != nil {
			return nil, err
		}
		temporary, createErr := os.CreateTemp(directory, ".remove-*")
		if createErr != nil {
			return nil, createErr
		}
		defer os.Remove(temporary.Name())
		defer temporary.Close()
		if err := json.NewEncoder(temporary).Encode(applianceRemoveCommand{Timeout: timeout, Request: request}); err != nil {
			return nil, err
		}
		if err := temporary.Sync(); err != nil {
			return nil, err
		}
		if err := temporary.Close(); err != nil {
			return nil, err
		}
		if err := os.Link(temporary.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		encoded, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	var saved applianceRemoveCommand
	if err := json.Unmarshal(encoded, &saved); err != nil {
		return nil, fmt.Errorf("read original appliance removal command: %w", err)
	}
	request.DeadlineUnixMillis = saved.Request.GetDeadlineUnixMillis()
	if saved.Timeout != timeout || !proto.Equal(request, saved.Request) {
		return nil, errors.New("removal request ID belongs to a different confirmed command; repeat its original parameters")
	}
	parent, err := os.Open(directory)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	if err := parent.Sync(); err != nil {
		return nil, err
	}
	return saved.Request, nil
}
