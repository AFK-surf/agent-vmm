//go:build linux

package vmmprocess

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type Client struct {
	path, session string
	launch        string
	mu            sync.Mutex
	state         hostv1.VMState
	changes       chan hostv1.VMState
	spawned       bool
	pid           int
}

func Start(ctx context.Context, config Config) (*Client, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	c := &Client{path: socketPath(config), session: hex.EncodeToString(id[:]), changes: make(chan hostv1.VMState, 8)}
	// Only a failed transport dial permits starting a new child. A live endpoint
	// rejecting adoption must never cause a competing VM to open the same disk.
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.path)
	if err == nil {
		conn.Close()
		adopted, e := c.claim(ctx, config)
		if e != nil {
			return nil, e
		}
		if adopted.State() == hostv1.VMState_VM_STATE_STOPPED || adopted.State() == hostv1.VMState_VM_STATE_FAILED {
			if e = adopted.retired(ctx); e != nil {
				return nil, e
			}
			return Start(ctx, config)
		}
		return adopted, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !errors.Is(err, os.ErrNotExist) && !isRefused(err) {
		return nil, err
	}
	// An exiting VMM closes its endpoint before it releases the instance lock.
	if err = c.retired(ctx); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(config.InstanceDirectory, 0700); err != nil {
		return nil, err
	}
	files := []*os.File{}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	read, write, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	files = append(files, read)
	if err = writeMessage(write, launchSpec{Config: config, Launch: c.session}); err != nil {
		write.Close()
		return nil, err
	}
	write.Close()
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer readyRead.Close()
	files = append(files, readyWrite)
	for i, path := range []string{config.KernelPath, config.InitramfsPath, config.RootFSPath, config.StatePath, config.LegacyStatePath} {
		if path == "" && i == 4 {
			break
		}
		mode := os.O_RDONLY
		if i == 3 {
			mode = os.O_RDWR
		}
		f, e := os.OpenFile(path, mode, 0)
		if e != nil {
			return nil, e
		}
		files = append(files, f)
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	// The VMM keeps its own session so Host signals, terminal hangups, and
	// service restarts do not reach the running VM.
	command := exec.Command(executable, "__vmm")
	command.ExtraFiles = files
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	log, err := os.OpenFile(config.SerialLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	defer log.Close()
	command.Stdout = log
	command.Stderr = log
	if err = command.Start(); err != nil {
		return nil, err
	}
	readyWrite.Close()
	c.launch = c.session
	exited := make(chan struct{})
	var exitErr error
	go func() { exitErr = command.Wait(); close(exited) }()
	claimed := false
	defer func() {
		if !claimed {
			_ = command.Process.Kill()
			<-exited
		}
	}()
	ready := make(chan bool, 1)
	go func() { n, _ := readyRead.Read(make([]byte, 1)); ready <- n == 1 }()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ok := <-ready:
		if !ok {
			<-exited
			return nil, fmt.Errorf("VMM exited before ready: %v (see %s)", exitErr, config.SerialLog)
		}
	}
	client, err := c.claim(ctx, config)
	claimed = err == nil && client.spawned
	return client, err
}
func (c *Client) claim(ctx context.Context, config Config) (*Client, error) {
	conn, r, err := exchange(ctx, c.path, request{Operation: "claim", Session: c.session, Config: &config})
	if err != nil {
		return nil, err
	}
	conn.Close()
	c.spawned = c.launch != "" && c.launch == r.Launch
	c.pid = r.PID
	c.setState(r.State)
	go c.observe()
	return c, nil
}
func (c *Client) setState(state hostv1.VMState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == state {
		return
	}
	c.state = state
	select {
	case c.changes <- state:
	default:
		select {
		case <-c.changes:
		default:
		}
		c.changes <- state
	}
}

// PID identifies the VMM process for resource accounting.
func (c *Client) PID() int                                  { return c.pid }
func (c *Client) State() hostv1.VMState                     { c.mu.Lock(); defer c.mu.Unlock(); return c.state }
func (c *Client) StateChangedNotify() <-chan hostv1.VMState { return c.changes }
func (c *Client) observe() {
	conn, r, err := exchange(context.Background(), c.path, request{Operation: "wait", Session: c.session})
	if err != nil {
		state := c.State()
		if state != hostv1.VMState_VM_STATE_STOPPED && state != hostv1.VMState_VM_STATE_FAILED {
			c.setState(hostv1.VMState_VM_STATE_FAILED)
		}
		return
	}
	conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.retired(ctx); err != nil {
		c.setState(hostv1.VMState_VM_STATE_FAILED)
		return
	}
	c.setState(r.State)
}
func (c *Client) retired(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		running, err := Running(filepath.Dir(c.path))
		if err != nil {
			return err
		}
		if !running {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (c *Client) Spawned() bool { return c.spawned }
func (c *Client) Dial(ctx context.Context, port uint32) (net.Conn, error) {
	conn, _, err := exchange(ctx, c.path, request{Operation: "dial", Session: c.session, Port: port})
	if err != nil {
		return nil, err
	}
	return conn, nil
}
func (c *Client) Listen(port uint32) (net.Listener, error) {
	return &listener{client: c, port: port, closed: make(chan struct{})}, nil
}
func (c *Client) CanStop() bool {
	return c.State() == hostv1.VMState_VM_STATE_RUNNING || c.State() == hostv1.VMState_VM_STATE_STARTING
}
func (c *Client) CanRequestStop() bool       { return false }
func (c *Client) RequestStop() (bool, error) { return false, nil }
func (c *Client) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, r, err := exchange(ctx, c.path, request{Operation: "stop", Session: c.session})
	if err != nil {
		return err
	}
	conn.Close()
	if err = c.retired(ctx); err != nil {
		return err
	}
	c.setState(r.State)
	return nil
}

type listener struct {
	client *Client
	port   uint32
	closed chan struct{}
	once   sync.Once
}

func (l *listener) Accept() (net.Conn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-l.closed:
			cancel()
		case <-ctx.Done():
		}
	}()
	conn, _, err := exchange(ctx, l.client.path, request{Operation: "accept", Session: l.client.session, Port: l.port})
	if err != nil {
		select {
		case <-l.closed:
			return nil, net.ErrClosed
		default:
		}
		return nil, err
	}
	return conn, nil
}
func (l *listener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *listener) Addr() net.Addr { return &net.UnixAddr{Name: l.client.path, Net: "unix"} }
