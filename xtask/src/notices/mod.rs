mod cargo;
mod go;

use anyhow::{Context, Result, bail, ensure};
use bstr::ByteSlice;
use regex::Regex;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufWriter, ErrorKind, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::LazyLock,
};
use walkdir::WalkDir;

type Texts = BTreeMap<String, Vec<u8>>;
static LICENSE_ID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[A-Za-z0-9][A-Za-z0-9.+-]*").unwrap());

#[derive(Default)]
struct Notice {
    section: &'static str,
    name: String,
    version: String,
    license: String,
    origin: String,
    texts: Texts,
}

fn license_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [
        "license",
        "licence",
        "copying",
        "notice",
        "copyright",
        "unlicense",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix))
}

fn files(dir: &Path) -> Result<Texts> {
    let mut texts = Texts::new();
    for item in WalkDir::new(dir)
        .into_iter()
        .filter_entry(|e| !(e.file_type().is_dir() && e.file_name() == "testdata"))
    {
        let e = item?;
        if e.file_type().is_file() && license_name(&e.file_name().to_string_lossy()) {
            let key = e.path().strip_prefix(dir)?.to_string_lossy().into_owned();
            texts.insert(key, fs::read(e.path())?);
        }
    }
    Ok(texts)
}

fn fallback(expression: &str, dir: &Path, required: bool) -> Result<Texts> {
    let mut texts = Texts::new();
    for id in LICENSE_ID.find_iter(expression).map(|m| m.as_str()) {
        if matches!(id, "AND" | "OR" | "WITH") {
            continue;
        }
        match fs::read(dir.join(format!("{id}.txt"))) {
            Ok(data) => {
                texts.insert(id.into(), data);
            }
            Err(e) if !required && e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("missing license text {id}")),
        }
    }
    ensure!(!required || !texts.is_empty(), "missing license expression");
    Ok(texts)
}

fn apk(data: &str, licenses: &Path) -> Result<Vec<Notice>> {
    let mut rows = Vec::new();
    for paragraph in data.trim().split("\n\n") {
        let mut n = Notice {
            section: "Alpine",
            ..Default::default()
        };
        for line in paragraph.lines() {
            if let Some((tag, value)) = line.split_once(':') {
                match tag {
                    "P" => n.name = value.into(),
                    "V" => n.version = value.into(),
                    "L" => n.license = value.into(),
                    "o" => n.origin = value.into(),
                    _ => {}
                }
            }
        }
        if !n.name.is_empty() {
            n.texts = fallback(&n.license, licenses, false)?;
            rows.push(n);
        }
    }
    Ok(rows)
}

fn text(out: &mut impl Write, name: &str, bytes: &[u8]) -> Result<()> {
    writeln!(out, "\n--- {name} ---")?;
    out.write_all(bytes.trim())?;
    writeln!(out)?;
    Ok(())
}

fn render(mut rows: Vec<Notice>, mut out: impl Write) -> Result<()> {
    rows.sort_by(|a, b| (&a.section, &a.name, &a.version).cmp(&(&b.section, &b.name, &b.version)));
    rows.dedup_by(|a, b| (a.section, &a.name, &a.version) == (b.section, &b.name, &b.version));
    let mut shared = BTreeMap::new();
    writeln!(out, "THIRD-PARTY NOTICES\n")?;
    for section in ["Go", "Rust", "Alpine"] {
        writeln!(out, "{section}\n{}", "=".repeat(section.len()))?;
        for n in rows.iter().filter(|n| n.section == section) {
            writeln!(out, "\n{}@{}", n.name, n.version)?;
            if !n.license.is_empty() {
                writeln!(out, "License: {}", n.license)?;
            }
            if !n.origin.is_empty() {
                writeln!(out, "Origin: {}", n.origin)?;
            }
            for (name, bytes) in &n.texts {
                if LICENSE_ID
                    .find_iter(&n.license)
                    .any(|id| id.as_str() == name)
                {
                    shared.insert(name, bytes);
                } else {
                    text(&mut out, name, bytes)?;
                }
            }
        }
        writeln!(out)?;
    }
    if !shared.is_empty() {
        writeln!(out, "License texts\n=============")?;
        for (id, bytes) in shared {
            text(&mut out, id, bytes)?;
        }
    }
    out.flush()?;
    Ok(())
}

pub fn run(mut args: impl Iterator<Item = String>) -> Result<()> {
    let mut output = None;
    let mut licenses = PathBuf::from("third_party/licenses");
    let mut target = String::new();
    let (mut extras, mut binaries, mut manifests, mut databases) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    while let Some(arg) = args.next() {
        let flag = arg
            .strip_prefix("--")
            .or_else(|| arg.strip_prefix('-'))
            .context("expected a notice flag")?;
        let (flag, inline) = flag
            .split_once('=')
            .map_or((flag, None), |(k, v)| (k, Some(v.to_owned())));
        let value = inline
            .or_else(|| args.next())
            .with_context(|| format!("missing argument for -{flag}"))?;
        match flag {
            "o" => output = Some(value),
            "licenses" => licenses = value.into(),
            "cargo-target" => target = value,
            "extra" => extras.push(value),
            "go-binary" => binaries.push(value),
            "cargo-manifest" => manifests.push(value),
            "apk-db" => databases.push(value),
            _ => bail!("unknown notices flag -{flag}"),
        }
    }
    let output = output
        .filter(|s| !s.is_empty())
        .context("notices requires -o FILE")?;
    ensure!(
        manifests.is_empty() || !target.is_empty(),
        "-cargo-manifest requires -cargo-target"
    );
    let mut rows = Vec::new();
    for extra in extras {
        let (name, id) = extra
            .split_once('=')
            .filter(|(k, v)| !k.is_empty() && !v.is_empty())
            .context("expected -extra NAME=SPDX-ID")?;
        rows.push(Notice {
            section: "Go",
            name: name.into(),
            license: id.into(),
            texts: fallback(id, &licenses, true)?,
            ..Default::default()
        });
    }
    let mut modules = BTreeSet::new();
    for path in binaries {
        for item in WalkDir::new(path).follow_root_links(false) {
            let e = item?;
            if e.file_type().is_file() {
                let bytes = fs::read(e.path())?;
                if let Some(info) = crate::buildinfo::read(&bytes) {
                    modules.extend(
                        crate::buildinfo::modules(info)
                            .into_iter()
                            .map(|(p, v)| (p.to_owned(), v.to_owned())),
                    );
                }
            }
        }
    }
    let source = go::Source::from_env();
    for (name, version) in modules {
        let texts = source
            .licenses(&name, &version)
            .with_context(|| format!("Go module {name}@{version}"))?;
        ensure!(
            !texts.is_empty(),
            "Go module {name}@{version} has no license files"
        );
        rows.push(Notice {
            section: "Go",
            name,
            version,
            texts,
            ..Default::default()
        });
    }
    for manifest in manifests {
        let result = Command::new("cargo")
            .args([
                "metadata",
                "--locked",
                "--format-version",
                "1",
                "--manifest-path",
                &manifest,
                "--filter-platform",
                &target,
            ])
            .output()?;
        ensure!(
            result.status.success(),
            "cargo metadata for {manifest}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        rows.extend(cargo::notices(&result.stdout, &licenses)?);
    }
    for path in databases {
        rows.extend(apk(&fs::read_to_string(path)?, &licenses)?);
    }
    render(rows, BufWriter::new(fs::File::create(output)?))
}
