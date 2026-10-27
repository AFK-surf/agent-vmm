//go:build darwin || linux

// Package lifecycle owns installation, update, maintenance, and their durable request journal.
// Service-user business effects and fixed administrator service actions retain separate authority.
package lifecycle
