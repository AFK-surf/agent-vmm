use super::{Texts, files, license_name};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{
    fs,
    io::{Cursor, ErrorKind, Read, Write},
    path::{Path, PathBuf},
};
use zip::ZipArchive;

// The Go module zip specification limits both the archive and its uncompressed contents.
const MAX_MODULE_ZIP: u64 = 500 << 20;

pub(super) struct Source {
    modules: Option<PathBuf>,
    cache: Option<PathBuf>,
    agent: ureq::Agent,
}

fn escape(value: &str) -> String {
    let mut s = String::new();
    for c in value.chars() {
        if c.is_ascii_uppercase() {
            s.push('!');
        }
        s.push(c.to_ascii_lowercase());
    }
    s
}

fn valid_component(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-~+".contains(&c))
}

fn cached(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(data) => Ok(Some(data)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl Source {
    pub(super) fn from_env() -> Self {
        let tls = ureq::tls::TlsConfig::builder()
            .root_certs(ureq::tls::RootCerts::PlatformVerifier)
            .build();
        Self {
            modules: std::env::var_os("GO_MOD_CACHE_DIR").map(Into::into),
            cache: std::env::var_os("GO_PROXY_CACHE_DIR").map(Into::into),
            agent: ureq::Agent::config_builder()
                .https_only(true)
                .timeout_global(Some(std::time::Duration::from_secs(240)))
                .tls_config(tls)
                .build()
                .new_agent(),
        }
    }

    fn get(&self, relative: &str) -> Result<Vec<u8>> {
        for dir in [
            self.modules.as_ref().map(|p| p.join("cache/download")),
            self.cache.clone(),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(bytes) = cached(&dir.join(relative))? {
                return Ok(bytes);
            }
        }
        let url = format!("https://proxy.golang.org/{relative}");
        let bytes = self
            .agent
            .get(&url)
            .call()?
            .body_mut()
            .with_config()
            .limit(MAX_MODULE_ZIP)
            .read_to_vec()?;
        if let Some(dir) = &self.cache {
            let path = dir.join(relative);
            let parent = path.parent().context("missing proxy cache directory")?;
            fs::create_dir_all(parent)?;
            let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
            tmp.write_all(&bytes)?;
            tmp.persist(path)?;
        }
        Ok(bytes)
    }

    pub(super) fn licenses(&self, module: &str, version: &str) -> Result<Texts> {
        ensure!(
            module.split('/').all(valid_component) && valid_component(version),
            "invalid module path or version"
        );
        let path = escape(module);
        if let Some(dir) = &self.modules {
            let extracted = dir.join(format!("{path}@{}", escape(version)));
            if extracted.is_dir() {
                return files(&extracted);
            }
        }
        // APK builds sometimes record the main module's commit instead of its version.
        let version = if version.starts_with('v')
            && version.as_bytes().get(1).is_some_and(u8::is_ascii_digit)
            && version.contains('.')
        {
            version.to_owned()
        } else {
            #[derive(Deserialize)]
            struct Info {
                #[serde(rename = "Version")]
                version: String,
            }
            let info: Info =
                serde_json::from_slice(&self.get(&format!("{path}/@v/{}.info", escape(version)))?)?;
            ensure!(
                valid_component(&info.version),
                "invalid module proxy version"
            );
            info.version
        };
        let bytes = self.get(&format!("{path}/@v/{}.zip", escape(&version)))?;
        archive(&bytes, &format!("{module}@{version}/"))
    }
}

fn archive(bytes: &[u8], prefix: &str) -> Result<Texts> {
    let mut zip = ZipArchive::new(Cursor::new(bytes))?;
    let mut result = Texts::new();
    let mut remaining = MAX_MODULE_ZIP;
    for i in 0..zip.len() {
        let f = zip.by_index(i)?;
        let size = f.size();
        remaining = remaining
            .checked_sub(size)
            .context("module zip exceeds 500 MiB")?;
        let name = f.name()?;
        let relative = name
            .strip_prefix(prefix)
            .context("unexpected module zip prefix")?;
        let mut parts = relative.split('/').rev();
        if !f.is_file()
            || !license_name(parts.next().unwrap_or_default())
            || parts.any(|s| s == "testdata")
        {
            continue;
        }
        let key = relative.to_owned();
        let mut text = Vec::new();
        f.take(size + 1).read_to_end(&mut text)?;
        ensure!(
            text.len() as u64 == size,
            "incorrect zip entry size for {key}"
        );
        result.insert(key, text);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zip::{ZipWriter, write::SimpleFileOptions};

    #[test]
    fn revision_uses_canonical_proxy_version_and_escapes_uppercase() {
        let cache = tempfile::tempdir().unwrap();
        let versions = cache.path().join("example.org/!m!od/@v");
        fs::create_dir_all(&versions).unwrap();
        fs::write(versions.join("abc123.info"), r#"{"Version":"v1.0.0-RC1"}"#).unwrap();
        let file = fs::File::create(versions.join("v1.0.0-!r!c1.zip")).unwrap();
        let mut zip = ZipWriter::new(file);
        zip.start_file(
            "example.org/MOd@v1.0.0-RC1/LICENCE",
            SimpleFileOptions::default(),
        )
        .unwrap();
        zip.write_all(b"license").unwrap();
        zip.finish().unwrap();
        let source = Source {
            cache: Some(cache.path().into()),
            modules: None,
            ..Source::from_env()
        };
        assert_eq!(
            source.licenses("example.org/MOd", "abc123").unwrap()["LICENCE"],
            b"license"
        );
    }
}
