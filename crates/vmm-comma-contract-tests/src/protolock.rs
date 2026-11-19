//! The pinned protobuf surface of agent-vmm and the names Cue depends on.
//!
//! `golden_vectors/proto_lock.json` records every field number, type, label,
//! enum value and RPC of agent-vmm `89b0c0a` (the Host pin in Cue
//! `.github/agent-vmm-host.json`). [`parse_tree`] reads a directory of
//! `.proto` files and [`compare`] reports each incompatible difference.
//! Additions are compatible; removals, renumbering, type or label changes and
//! streaming changes are not.

use serde_json::{json, Map, Value};
use std::path::Path;

/// Parses every `.proto` file under `root` into the lock's `packages` shape.
pub fn parse_tree(root: &Path) -> Result<Value, String> {
    let mut files = Vec::new();
    collect(root, &mut files)?;
    files.sort();
    if files.is_empty() {
        return Err(format!("no .proto files under {}", root.display()));
    }
    let mut packages = Map::new();
    for file in files {
        let text = std::fs::read_to_string(&file).map_err(|error| format!("{}: {error}", file.display()))?;
        parse_file(&text, &mut packages).map_err(|error| format!("{}: {error}", file.display()))?;
    }
    Ok(Value::Object(packages))
}

fn collect(directory: &Path, files: &mut Vec<std::path::PathBuf>) -> Result<(), String> {
    let entries = std::fs::read_dir(directory).map_err(|error| format!("{}: {error}", directory.display()))?;
    for entry in entries {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.is_dir() {
            collect(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "proto") {
            files.push(path);
        }
    }
    Ok(())
}

/// Splits proto source into `(terminator, statement)` tokens. Comments are
/// dropped and string literals are kept whole.
fn statements(text: &str) -> Result<Vec<(char, String)>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut rest = text;
    while let Some(character) = rest.chars().next() {
        if character == '"' {
            let end = rest[1..].find('"').ok_or("unterminated string")? + 2;
            current.push_str(&rest[..end]);
            rest = &rest[end..];
        } else if rest.starts_with("//") {
            rest = rest.find('\n').map_or("", |index| &rest[index..]);
        } else if rest.starts_with("/*") {
            rest = &rest[rest.find("*/").ok_or("unterminated comment")? + 2..];
        } else {
            if matches!(character, ';' | '{' | '}') {
                tokens.push((character, current.split_whitespace().collect::<Vec<_>>().join(" ")));
                current.clear();
            } else {
                current.push(character);
            }
            rest = &rest[character.len_utf8()..];
        }
    }
    Ok(tokens)
}

fn parse_file(text: &str, packages: &mut Map<String, Value>) -> Result<(), String> {
    let mut package: Option<String> = None;
    let mut stack: Vec<(String, String)> = Vec::new();
    let mut messages: Vec<(String, Map<String, Value>)> = Vec::new();
    let mut enums: Vec<(String, Map<String, Value>)> = Vec::new();
    let mut services: Vec<(String, Map<String, Value>)> = Vec::new();
    for (terminator, body) in statements(text)? {
        if terminator == '}' {
            stack.pop().ok_or("unbalanced }")?;
            continue;
        }
        let words: Vec<&str> = body.split(' ').filter(|word| !word.is_empty()).collect();
        let head = words.first().copied().unwrap_or("");
        if terminator == '{' {
            match head {
                "extend" | "oneof" => stack.push((head.into(), words.get(1).copied().unwrap_or("").into())),
                "message" | "enum" | "service" => {
                    let name = words.get(1).ok_or("block without a name")?.to_string();
                    if head != "service" && !stack.is_empty() {
                        return Err(format!("nested {head} {name} is not supported"));
                    }
                    match head {
                        "message" => messages.push((name.clone(), Map::new())),
                        "enum" => enums.push((name.clone(), Map::new())),
                        _ => services.push((name.clone(), Map::new())),
                    }
                    stack.push((head.into(), name));
                }
                "rpc" => {
                    let (name, rpc) = parse_rpc(&body)?;
                    services.last_mut().ok_or("rpc outside a service")?.1.insert(name.clone(), rpc);
                    stack.push(("rpcbody".into(), name));
                }
                _ => return Err(format!("unknown block: {body}")),
            }
            continue;
        }
        if body.is_empty() || matches!(head, "syntax" | "import" | "option" | "reserved") {
            continue;
        }
        if head == "package" {
            package = Some(words.get(1).ok_or("package without a name")?.to_string());
            continue;
        }
        let (kind, name) = stack.last().ok_or_else(|| format!("top-level statement: {body}"))?.clone();
        match kind.as_str() {
            "service" => {
                let (name, rpc) = parse_rpc(&body)?;
                services.last_mut().ok_or("rpc outside a service")?.1.insert(name, rpc);
            }
            "enum" => {
                let (value_name, number) = body.split_once('=').ok_or_else(|| format!("bad enum value: {body}"))?;
                let number: i64 = number.split('[').next().unwrap_or("").trim().parse().map_err(|_| format!("bad enum number: {body}"))?;
                enums.last_mut().ok_or("enum value outside an enum")?.1.insert(value_name.trim().into(), json!(number));
            }
            "message" | "oneof" => {
                let (declaration, number) = body.split_once('=').ok_or_else(|| format!("bad field: {body}"))?;
                let number: u64 = number.split('[').next().unwrap_or("").trim().parse().map_err(|_| format!("bad field number: {body}"))?;
                let mut parts: Vec<&str> = declaration.split(' ').filter(|part| !part.is_empty()).collect();
                let label = if matches!(parts.first(), Some(&"optional") | Some(&"repeated")) { parts.remove(0) } else { "" };
                let [field_type, field_name] = parts.as_slice() else { return Err(format!("bad field: {body}")) };
                let mut entry = Map::new();
                entry.insert("number".into(), json!(number));
                entry.insert("type".into(), json!(field_type));
                entry.insert("label".into(), json!(label));
                if kind == "oneof" {
                    entry.insert("oneof".into(), json!(name));
                }
                messages.last_mut().ok_or("field outside a message")?.1.insert(field_name.to_string(), Value::Object(entry));
            }
            _ => {}
        }
    }
    let package = package.ok_or("no package")?;
    let target = packages.entry(package.clone()).or_insert_with(|| json!({"messages": {}, "enums": {}, "services": {}}));
    for (kind, values) in [("messages", messages), ("enums", enums), ("services", services)] {
        let section = target[kind].as_object_mut().expect("section is an object");
        for (name, value) in values {
            if section.insert(name.clone(), Value::Object(value)).is_some() {
                return Err(format!("duplicate {package}.{name}"));
            }
        }
    }
    Ok(())
}

fn parse_rpc(body: &str) -> Result<(String, Value), String> {
    let bad = || format!("bad rpc: {body}");
    let rest = body.strip_prefix("rpc ").ok_or_else(bad)?;
    let (name, rest) = rest.split_once('(').ok_or_else(bad)?;
    let (request, rest) = rest.split_once(')').ok_or_else(bad)?;
    let (_, rest) = rest.split_once('(').ok_or_else(bad)?;
    let (response, _) = rest.split_once(')').ok_or_else(bad)?;
    let side = |text: &str| {
        let text = text.trim();
        match text.strip_prefix("stream ") {
            Some(name) => (name.trim().to_string(), true),
            None => (text.to_string(), false),
        }
    };
    let (request, client_streaming) = side(request);
    let (response, server_streaming) = side(response);
    Ok((
        name.trim().to_string(),
        json!({"request": request, "client_streaming": client_streaming, "response": response, "server_streaming": server_streaming}),
    ))
}

/// Every way `actual` (a parsed tree) breaks the wire contract of `lock`.
pub fn compare(lock: &Value, actual: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let empty = Map::new();
    for (package, expected) in lock["packages"].as_object().unwrap_or(&empty) {
        let Some(found) = actual.get(package) else {
            violations.push(format!("package {package} is missing"));
            continue;
        };
        for kind in ["messages", "enums", "services"] {
            for (name, members) in expected[kind].as_object().unwrap_or(&empty) {
                let Some(found_members) = found[kind].get(name) else {
                    violations.push(format!("{package}.{name} ({kind}) is missing"));
                    continue;
                };
                for (member, value) in members.as_object().unwrap_or(&empty) {
                    match found_members.get(member) {
                        None => violations.push(format!("{package}.{name}.{member} is missing")),
                        Some(found_value) if found_value != value => {
                            violations.push(format!("{package}.{name}.{member} changed: pinned {value}, found {found_value}"))
                        }
                        Some(_) => {}
                    }
                }
            }
        }
    }
    violations
}

/// protoc's JSON name of a field: underscores removed, next letter upper-cased.
pub fn json_name(field: &str) -> String {
    let mut result = String::with_capacity(field.len());
    let mut upper = false;
    for character in field.chars() {
        if character == '_' {
            upper = true;
        } else if upper {
            result.extend(character.to_uppercase());
            upper = false;
        } else {
            result.push(character);
        }
    }
    result
}

fn lookup<'a>(tree: &'a Value, kind: &str, full_name: &str) -> Option<&'a Map<String, Value>> {
    let (package, name) = full_name.rsplit_once('.')?;
    tree.get(package)?.get(kind)?.get(name)?.as_object()
}

/// Checks `golden_vectors/remote_v1/cue_proto_usage.json` against a parsed
/// tree: each name Cue uses must exist in the protocol under test.
pub fn check_usage(usage: &Value, tree: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    let strings = |value: &Value| -> Vec<String> {
        value.as_array().map(|items| items.iter().filter_map(Value::as_str).map(str::to_owned).collect()).unwrap_or_default()
    };
    let items = |value: &Value| value.as_array().cloned().unwrap_or_default();
    for entry in items(&usage["json_fields"]) {
        let message = entry["message"].as_str().unwrap_or_default();
        match lookup(tree, "messages", message) {
            None => problems.push(format!("message {message} is missing ({})", entry["source"])),
            Some(fields) => {
                let names: Vec<String> = fields.keys().map(|field| json_name(field)).collect();
                for key in strings(&entry["json"]) {
                    if !names.contains(&key) {
                        problems.push(format!("{message} has no JSON field {key} ({})", entry["source"]));
                    }
                }
            }
        }
    }
    for entry in items(&usage["enum_literals"]) {
        let name = entry["enum"].as_str().unwrap_or_default();
        match lookup(tree, "enums", name) {
            None => problems.push(format!("enum {name} is missing ({})", entry["source"])),
            Some(values) => {
                for value in strings(&entry["values"]) {
                    if !values.contains_key(&value) {
                        problems.push(format!("{name} has no value {value} ({})", entry["source"]));
                    }
                }
            }
        }
    }
    let reasons = &usage["host_error_reasons"];
    let reason_enum = lookup(tree, "enums", reasons["enum"].as_str().unwrap_or_default());
    for entry in items(&reasons["mapped"]) {
        let literal = format!("ERROR_REASON_{}", entry["reason"].as_str().unwrap_or_default());
        if !reason_enum.is_some_and(|values| values.contains_key(&literal)) {
            problems.push(format!("host error reason {literal} is missing ({})", entry["source"]));
        }
    }
    let runtime = &usage["gateway_runtime_rpcs"];
    let service_name = runtime["service"].as_str().unwrap_or_default();
    let package = service_name.rsplit_once('.').map_or("", |(package, _)| package);
    let service = lookup(tree, "services", service_name);
    for call in items(&runtime["calls"]) {
        let rpc = call["rpc"].as_str().unwrap_or_default();
        let Some(found) = service.and_then(|service| service.get(rpc)) else {
            problems.push(format!("{service_name}.{rpc} is missing ({})", call["source"]));
            continue;
        };
        if found["request"] != call["request"] {
            problems.push(format!("{service_name}.{rpc} request is {}, expected {}", found["request"], call["request"]));
        }
        let request = format!("{package}.{}", call["request"].as_str().unwrap_or_default());
        let fields = lookup(tree, "messages", &request);
        for field in strings(&call["request_fields"]) {
            if !fields.is_some_and(|fields| fields.contains_key(&field)) {
                problems.push(format!("{request} has no field {field} ({})", call["source"]));
            }
        }
    }
    for entry in items(&runtime["nested_fields"]) {
        let message = entry["message"].as_str().unwrap_or_default();
        let fields = lookup(tree, "messages", message);
        for field in strings(&entry["proto"]) {
            if !fields.is_some_and(|fields| fields.contains_key(&field)) {
                problems.push(format!("{message} has no field {field} ({})", entry["source"]));
            }
        }
    }
    for entry in items(&usage["services"]) {
        let name = entry["service"].as_str().unwrap_or_default();
        let Some(found) = lookup(tree, "services", name) else {
            problems.push(format!("service {name} is missing ({})", entry["source"]));
            continue;
        };
        let empty = Map::new();
        for (rpc, streaming) in entry["rpcs"].as_object().unwrap_or(&empty) {
            let matches = found.get(rpc).is_some_and(|rpc| rpc["client_streaming"] == streaming[0] && rpc["server_streaming"] == streaming[1]);
            if !matches {
                problems.push(format!("{name}.{rpc} is missing or changed its streaming shape ({})", entry["source"]));
            }
        }
    }
    problems
}
