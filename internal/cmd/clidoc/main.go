// clidoc generates the managed CLI reference from the command table.
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func main() {
	if err := generate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generate() error {
	source, err := parser.ParseFile(token.NewFileSet(), "cmd/agent-vmm/managed_cli.go", nil, 0)
	if err != nil {
		return err
	}
	commands := make(map[string][]string)
	ast.Inspect(source, func(node ast.Node) bool {
		value, ok := node.(*ast.ValueSpec)
		if !ok || len(value.Names) != 1 || value.Names[0].Name != "managedCommands" {
			return true
		}
		table := value.Values[0].(*ast.CompositeLit)
		for _, element := range table.Elts {
			entry := element.(*ast.KeyValueExpr)
			name, _ := strconv.Unquote(entry.Key.(*ast.BasicLit).Value)
			for _, field := range entry.Value.(*ast.CompositeLit).Elts {
				field := field.(*ast.KeyValueExpr)
				if field.Key.(*ast.Ident).Name != "flags" {
					continue
				}
				for _, flag := range field.Value.(*ast.CompositeLit).Elts {
					flag, _ := strconv.Unquote(flag.(*ast.BasicLit).Value)
					commands[name] = append(commands[name], "`--"+flag+"`")
				}
			}
		}
		return false
	})
	if len(commands) == 0 {
		return fmt.Errorf("managed command table is missing")
	}
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	var output bytes.Buffer
	output.WriteString(`# Managed CLI reference

<!-- Generated from cmd/agent-vmm/managed_cli.go by make cli-docs. -->

The formal Host bundle includes these commands. Each command accepts only its listed options.
Use ` + "`--json`" + ` for the structured result. Unknown or incomplete arguments return ` + "`invalid_arguments`" + ` and exit code 2.

| Command | Options |
| --- | --- |
`)
	for _, name := range names {
		fmt.Fprintf(&output, "| `agent-vmm %s` | %s |\n", name, strings.Join(commands[name], ", "))
	}

	path := filepath.Join("docs", "reference", "cli.md")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, output.Bytes(), 0644)
}
