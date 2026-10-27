// Command protodoc writes the API reference from a protobuf descriptor set.
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: protodoc DESCRIPTOR_SET OUTPUT_DIRECTORY")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(input, output string) error {
	raw, err := os.ReadFile(input)
	if err != nil {
		return err
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &set); err != nil {
		return err
	}
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}
	slices.SortFunc(set.File, func(a, b *descriptorpb.FileDescriptorProto) int { return strings.Compare(a.GetName(), b.GetName()) })
	var index bytes.Buffer
	index.WriteString("# API reference\n\nGenerated from protobuf declarations and comments. Run `make proto-docs` to update these pages.\n\n")
	for _, file := range set.File {
		if !strings.HasPrefix(file.GetPackage(), "agentvmm.") || strings.HasPrefix(file.GetName(), "internal/") {
			continue
		}
		name := strings.ReplaceAll(strings.TrimSuffix(file.GetName(), ".proto"), "/", "-") + ".md"
		var b bytes.Buffer
		fmt.Fprintf(&b, "# `%s`\n\nSource: `%s`.\n\n", file.GetPackage(), file.GetName())
		if strings.Contains(file.GetPackage(), ".vmmd.") || strings.Contains(file.GetPackage(), ".connector.") {
			b.WriteString("Private IPC. This package is not a public integration interface.\n\n")
		}
		comments := map[string]string{}
		for _, location := range file.GetSourceCodeInfo().GetLocation() {
			lines := strings.Split(strings.TrimSpace(location.GetLeadingComments()+location.GetTrailingComments()), "\n")
			for i := range lines {
				lines[i] = strings.TrimPrefix(lines[i], " ")
			}
			comments[fmt.Sprint(location.GetPath())] = strings.Join(lines, "\n")
		}
		comment := func(path ...int32) {
			if text := comments[fmt.Sprint(path)]; text != "" {
				fmt.Fprintf(&b, "%s\n\n", text)
			}
		}
		for i, service := range file.GetService() {
			fmt.Fprintf(&b, "## Service `%s`\n\n", service.GetName())
			comment(6, int32(i))
			for j, method := range service.GetMethod() {
				fmt.Fprintf(&b, "### `%s`\n\n", method.GetName())
				comment(6, int32(i), 2, int32(j))
				request, response := method.GetInputType(), method.GetOutputType()
				if method.GetClientStreaming() {
					request = "stream " + request
				}
				if method.GetServerStreaming() {
					response = "stream " + response
				}
				fmt.Fprintf(&b, "`%s` → `%s`\n\n", request, response)
			}
		}
		enumeration := func(e *descriptorpb.EnumDescriptorProto, path []int32, prefix string) {
			fmt.Fprintf(&b, "## Enum `%s%s`\n\n", prefix, e.GetName())
			comment(path...)
			for j, v := range e.GetValue() {
				fmt.Fprintf(&b, "### `%s = %d`\n\n", v.GetName(), v.GetNumber())
				comment(append(slices.Clone(path), 2, int32(j))...)
			}
		}
		var message func(*descriptorpb.DescriptorProto, []int32, string)
		message = func(m *descriptorpb.DescriptorProto, path []int32, prefix string) {
			fmt.Fprintf(&b, "## Message `%s%s`\n\n", prefix, m.GetName())
			comment(path...)
			fieldTable := false
			for i, f := range m.GetField() {
				description := comments[fmt.Sprint(append(slices.Clone(path), 2, int32(i)))]
				if description == "" {
					continue
				}
				if !fieldTable {
					b.WriteString("| Field | Number | Type | Description |\n| --- | --- | --- | --- |\n")
					fieldTable = true
				}
				kind := strings.ToLower(strings.TrimPrefix(f.GetType().String(), "TYPE_"))
				if f.GetTypeName() != "" {
					kind = f.GetTypeName()
				}
				if f.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
					kind = "repeated " + kind
				} else if f.GetProto3Optional() {
					kind = "optional " + kind
				} else if f.OneofIndex != nil {
					kind = "oneof " + m.GetOneofDecl()[f.GetOneofIndex()].GetName() + ": " + kind
				}
				description = strings.ReplaceAll(strings.ReplaceAll(description, "\n", " "), "|", "\\|")
				fmt.Fprintf(&b, "| `%s` | %d | `%s` | %s |\n", f.GetName(), f.GetNumber(), kind, description)
			}
			b.WriteString("\n")
			for i, e := range m.GetEnumType() {
				enumeration(e, append(slices.Clone(path), 4, int32(i)), prefix+m.GetName()+".")
			}
			for i, nested := range m.GetNestedType() {
				message(nested, append(slices.Clone(path), 3, int32(i)), prefix+m.GetName()+".")
			}
		}
		for i, m := range file.GetMessageType() {
			message(m, []int32{4, int32(i)}, "")
		}
		for i, e := range file.GetEnumType() {
			enumeration(e, []int32{5, int32(i)}, "")
		}
		if err := os.WriteFile(filepath.Join(output, name), append(bytes.TrimRight(b.Bytes(), "\n"), '\n'), 0644); err != nil {
			return err
		}
		fmt.Fprintf(&index, "- [%s](%s)\n", file.GetName(), name)
	}
	return os.WriteFile(filepath.Join(output, "README.md"), index.Bytes(), 0644)
}
