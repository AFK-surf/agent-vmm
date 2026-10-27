//go:build darwin || linux

// Package host composes Host owners and adapts the local RPC listeners.
// Config, New, Start, and Close define construction and lifetime.
// Each domain owner keeps its state and locks, with dependencies directed toward shared primitives.
package host
