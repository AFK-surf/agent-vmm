// Package control owns Guest runtime mutations. Locks follow this order:
// mutation dedupe, materialization slot, disk budget, namespace, container.
package control
