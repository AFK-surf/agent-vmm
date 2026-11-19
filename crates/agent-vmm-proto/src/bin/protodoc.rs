//! Writes the API reference from a protobuf descriptor set.

use prost::Message;
use prost_types::field_descriptor_proto::Label;
use prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorSet};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::{env, fs, process};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: protodoc DESCRIPTOR_SET OUTPUT_DIRECTORY");
        process::exit(2);
    }
    if let Err(error) = run(Path::new(&args[1]), Path::new(&args[2])) {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run(input: &Path, output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut set = FileDescriptorSet::decode(fs::read(input)?.as_slice())?;
    fs::create_dir_all(output)?;
    set.file.sort_by(|a, b| a.name().cmp(b.name()));
    let mut index = String::from(
        "# API reference\n\nGenerated from protobuf declarations and comments. Run `make proto-docs` to update these pages.\n\n",
    );
    for file in &set.file {
        if !file.package().starts_with("agentvmm.") || file.name().starts_with("internal/") {
            continue;
        }
        let name = format!("{}.md", file.name().strip_suffix(".proto").unwrap_or(file.name()).replace('/', "-"));
        let comments: HashMap<Vec<i32>, String> = file
            .source_code_info
            .iter()
            .flat_map(|info| &info.location)
            .map(|location| {
                let text = format!("{}{}", location.leading_comments(), location.trailing_comments());
                let lines: Vec<&str> =
                    text.trim().split('\n').map(|line| line.strip_prefix(' ').unwrap_or(line)).collect();
                (location.path.clone(), lines.join("\n"))
            })
            .collect();
        let mut page = Page { text: String::new(), comments };
        write!(page.text, "# `{}`\n\nSource: `{}`.\n\n", file.package(), file.name())?;
        if file.package().contains(".vmmd.") || file.package().contains(".connector.") {
            page.text.push_str("Private IPC. This package is not a public integration interface.\n\n");
        }
        for (i, service) in file.service.iter().enumerate() {
            write!(page.text, "## Service `{}`\n\n", service.name())?;
            page.comment(&[6, i as i32]);
            for (j, method) in service.method.iter().enumerate() {
                write!(page.text, "### `{}`\n\n", method.name())?;
                page.comment(&[6, i as i32, 2, j as i32]);
                let stream = |streaming: bool| if streaming { "stream " } else { "" };
                write!(
                    page.text,
                    "`{}{}` → `{}{}`\n\n",
                    stream(method.client_streaming()),
                    method.input_type(),
                    stream(method.server_streaming()),
                    method.output_type()
                )?;
            }
        }
        for (i, message) in file.message_type.iter().enumerate() {
            page.message(message, vec![4, i as i32], "");
        }
        for (i, enumeration) in file.enum_type.iter().enumerate() {
            page.enumeration(enumeration, vec![5, i as i32], "");
        }
        let mut text = page.text.trim_end_matches('\n').to_owned();
        text.push('\n');
        fs::write(output.join(&name), text)?;
        writeln!(index, "- [{}]({name})", file.name())?;
    }
    fs::write(output.join("README.md"), index)?;
    Ok(())
}

struct Page {
    text: String,
    comments: HashMap<Vec<i32>, String>,
}

impl Page {
    fn comment(&mut self, path: &[i32]) {
        if let Some(text) = self.comments.get(path).filter(|text| !text.is_empty()) {
            self.text.push_str(text);
            self.text.push_str("\n\n");
        }
    }

    fn enumeration(&mut self, enumeration: &EnumDescriptorProto, path: Vec<i32>, prefix: &str) {
        self.text.push_str(&format!("## Enum `{prefix}{}`\n\n", enumeration.name()));
        self.comment(&path);
        for (j, value) in enumeration.value.iter().enumerate() {
            self.text.push_str(&format!("### `{} = {}`\n\n", value.name(), value.number()));
            self.comment(&[path.as_slice(), &[2, j as i32]].concat());
        }
    }

    fn message(&mut self, message: &DescriptorProto, path: Vec<i32>, prefix: &str) {
        self.text.push_str(&format!("## Message `{prefix}{}`\n\n", message.name()));
        self.comment(&path);
        let mut field_table = false;
        for (i, field) in message.field.iter().enumerate() {
            let Some(description) =
                self.comments.get(&[path.as_slice(), &[2, i as i32]].concat()).filter(|text| !text.is_empty())
            else {
                continue;
            };
            let description = description.replace('\n', " ").replace('|', "\\|");
            if !field_table {
                self.text.push_str("| Field | Number | Type | Description |\n| --- | --- | --- | --- |\n");
                field_table = true;
            }
            let mut kind = match field.type_name() {
                "" => field.r#type().as_str_name().trim_start_matches("TYPE_").to_lowercase(),
                name => name.to_owned(),
            };
            if field.label() == Label::Repeated {
                kind = format!("repeated {kind}");
            } else if field.proto3_optional() {
                kind = format!("optional {kind}");
            } else if let Some(oneof) = field.oneof_index {
                kind = format!("oneof {}: {kind}", message.oneof_decl[oneof as usize].name());
            }
            self.text.push_str(&format!("| `{}` | {} | `{kind}` | {description} |\n", field.name(), field.number()));
        }
        self.text.push('\n');
        let prefix = format!("{prefix}{}.", message.name());
        for (i, enumeration) in message.enum_type.iter().enumerate() {
            self.enumeration(enumeration, [path.as_slice(), &[4, i as i32]].concat(), &prefix);
        }
        for (i, nested) in message.nested_type.iter().enumerate() {
            self.message(nested, [path.as_slice(), &[3, i as i32]].concat(), &prefix);
        }
    }
}
