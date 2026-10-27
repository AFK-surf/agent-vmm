// SPDX-License-Identifier: Apache-2.0
// Derived from tinyrange/cc commit eed53ffb414a8bebcbb1d7934fab00558d78df00; modified for agent-vmm.
// Modified for agent-vmm; see ../LICENSE-cc.txt.
package fdt

type Property struct {
	Strings []string
	U32     []uint32
	U64     []uint64
	Bytes   []byte
	Flag    bool
}

func (p Property) Kind() string {
	switch {
	case len(p.Strings) > 0:
		return "strings"
	case len(p.U32) > 0:
		return "u32"
	case len(p.U64) > 0:
		return "u64"
	case len(p.Bytes) > 0:
		return "bytes"
	case p.Flag:
		return "flag"
	default:
		return ""
	}
}

func (p Property) DefinedCount() int {
	count := 0
	if len(p.Strings) > 0 {
		count++
	}
	if len(p.U32) > 0 {
		count++
	}
	if len(p.U64) > 0 {
		count++
	}
	if len(p.Bytes) > 0 {
		count++
	}
	if p.Flag {
		count++
	}
	return count
}

type Node struct {
	Name       string
	Properties map[string]Property
	Children   []Node
}
