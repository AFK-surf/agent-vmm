use crate::{backend::LocalDom0Backend, catalog::image_id, extent::ctl_error};
use sha2::{Digest as _, Sha256};
use std::{
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom},
    path::Path,
};
use vmm_core::{
    Digest, Error, ImageArtifact, ImageSource, Result,
    erofs_bundle::{
        EROFS_SUPER_MAGIC_LE, EROFS_SUPER_OFFSET, ErofsBundleCompression, ParsedErofsBundle,
    },
};

pub(crate) fn valid_token(token: &str) -> bool {
    token.len() == 32 && token.bytes().all(|c| c.is_ascii_hexdigit())
}

pub(crate) fn staged_at_startup(
    catalog: &crate::catalog::Catalog<'_>,
    dir: &Path,
) -> Result<Vec<String>> {
    let mut tokens = catalog.staged_tokens()?;
    for entry in dir.read_dir()? {
        let filename = entry?.file_name();
        if let Some(token) = filename.to_str().filter(|s| valid_token(s)) {
            tokens.push(token.to_owned());
        }
    }
    tokens.sort_unstable();
    tokens.dedup();
    Ok(tokens)
}

impl LocalDom0Backend {
    /// Allocated pool bytes, virtual pool capacity, and unpublished staging file bytes.
    pub fn disk_accounting(&self) -> Result<(u64, u64, u64)> {
        let (allocated, capacity) = self.pool.allocated_and_capacity_bytes()?;
        let mut staged = 0u64;
        for entry in self.cfg.staging.read_dir()? {
            let entry = entry?;
            if !entry.file_name().to_str().is_some_and(valid_token) {
                continue;
            }
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            if metadata.is_file() {
                staged = staged
                    .checked_add(metadata.len())
                    .ok_or(Error::Invalid("staging byte count"))?;
            }
        }
        Ok((allocated, capacity, staged))
    }

    pub fn discard_staged_image(&self, token: &str) -> Result<()> {
        if token.is_empty() {
            return Ok(());
        }
        if !valid_token(token) {
            return Err(Error::Invalid("staged image ID"));
        }
        match std::fs::remove_file(self.cfg.staging.join(token)) {
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            result => result?,
        }
        self.pool.discard(&format!("stage/{token}"))?;
        self.catalog().delete(&format!("stage/{token}"))
    }
    pub fn image_installed(&self, digest: Digest) -> Result<bool> {
        Ok(self.catalog().defaults(&image_id(digest))?.is_some())
    }

    /// Build a staged EROFS file and publish its activation parameters alongside it.
    pub fn stage_image(&self, source: &Path, digest: Digest) -> Result<ImageSource> {
        if self.cfg.builder_image == Some(digest) {
            let built = crate::image::build_trusted_builder(source, digest, &self.cfg.staging)?;
            return self.publish_stage(built, digest);
        }
        if let Some(parsed) =
            vmm_core::erofs_bundle::parse_and_validate_file(source, Some(digest))?
        {
            return self.stage_erofs_bundle(source, &parsed);
        }
        let built = crate::image::build(source, digest, &self.cfg.staging)?;
        self.publish_stage(built, digest)
    }

    fn stage_erofs_bundle(
        &self,
        source: &Path,
        parsed: &ParsedErofsBundle,
    ) -> Result<ImageSource> {
        let digest = parsed.manifest_digest;
        let erofs_bytes = parsed.header.erofs_bytes;
        if self.image_installed(digest)? {
            let installed_bytes = self
                .pool
                .lookup(&image_id(digest))
                .map(|exts| exts.iter().map(|e| e.sectors * 512).sum())
                .unwrap_or(erofs_bytes);
            return Ok(ImageSource {
                manifest_digest: digest,
                staged_extent: String::new(),
                physical_bytes: installed_bytes,
            });
        }
        let token = hex::encode(crate::keys::random::<16>()?.as_ref());
        let stage_obj = format!("stage/{token}");
        let extents = self.pool.allocate(&stage_obj, erofs_bytes)?;
        let write_result = (|| -> Result<()> {
            let mut file = std::fs::File::open(source)?;
            file.seek(SeekFrom::Start(parsed.payload_offset))?;
            let bounded = file.take(parsed.payload_len);
            let mut reader: Box<dyn Read> = match parsed.header.compression {
                ErofsBundleCompression::None => Box::new(bounded),
                ErofsBundleCompression::Zstd => Box::new(
                    zstd::stream::read::Decoder::new(BufReader::new(bounded))
                        .map_err(|_| Error::Invalid("invalid zstd EROFS payload"))?,
                ),
            };
            let mut limited = (&mut reader).take(erofs_bytes + 1);
            let mut sha = Sha256::new();
            let mut buf = vec![0u8; 1 << 20];
            let mut remaining_sectors = erofs_bytes / 512;
            let mut first_chunk = true;
            for ext in extents {
                if remaining_sectors == 0 {
                    break;
                }
                let mut pos = ext.sector;
                let end = ext.sector + ext.sectors;
                while pos < end && remaining_sectors > 0 {
                    let sectors = (end - pos).min(2048).min(remaining_sectors);
                    let byte_len = (sectors * 512) as usize;
                    let data = &mut buf[..byte_len];
                    let mut filled = 0usize;
                    while filled < byte_len {
                        match limited.read(&mut data[filled..]) {
                            Ok(0) => {
                                return Err(Error::Invalid(
                                    "EROFS bundle decompressed length mismatch",
                                ));
                            }
                            Ok(n) => filled += n,
                            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                            Err(_)
                                if parsed.header.compression == ErofsBundleCompression::Zstd =>
                            {
                                return Err(Error::Invalid("invalid zstd EROFS payload"));
                            }
                            Err(e) => return Err(Error::Io(e)),
                        }
                    }
                    if first_chunk {
                        first_chunk = false;
                        if data.len() < EROFS_SUPER_OFFSET + 4
                            || data[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4]
                                != EROFS_SUPER_MAGIC_LE
                        {
                            return Err(Error::Invalid("invalid EROFS superblock"));
                        }
                    }
                    sha.update(&*data);
                    self.ctl
                        .lock()
                        .map_err(|_| Error::Interrupted)?
                        .blk_write(self.pool.disk(), pos, data)
                        .map_err(ctl_error)?;
                    pos += sectors;
                    remaining_sectors -= sectors;
                }
            }
            let mut extra = [0u8; 1];
            match limited.read(&mut extra) {
                Ok(0) => {}
                Ok(_) => return Err(Error::Invalid("EROFS bundle decompressed length mismatch")),
                Err(_) if parsed.header.compression == ErofsBundleCompression::Zstd => {
                    return Err(Error::Invalid("invalid zstd EROFS payload"));
                }
                Err(e) => return Err(Error::Io(e)),
            }
            if Digest::from(sha.finalize()) != parsed.erofs_sha256 {
                return Err(Error::Invalid("EROFS bundle sha256 mismatch"));
            }
            self.ctl
                .lock()
                .map_err(|_| Error::Interrupted)?
                .blk_flush(self.pool.disk())
                .map_err(ctl_error)?;
            self.catalog()
                .put(&stage_obj, &(digest, parsed.header.defaults.clone(), erofs_bytes))?;
            Ok(())
        })();
        if let Err(err) = write_result {
            let _ = self.pool.discard(&stage_obj);
            let _ = self.catalog().delete(&stage_obj);
            return Err(err);
        }
        Ok(ImageSource {
            manifest_digest: digest,
            staged_extent: token,
            physical_bytes: erofs_bytes,
        })
    }
    /// Package a trusted host directory under its operator-assigned identity.
    pub fn stage_directory(&self, dir: &Path, digest: Digest) -> Result<ImageSource> {
        let built = crate::image::build_directory(dir, &self.cfg.staging)?;
        self.publish_stage(built, digest)
    }
    fn publish_stage(
        &self,
        built: crate::image::BuiltImage,
        digest: Digest,
    ) -> Result<ImageSource> {
        let size = built.file.as_file().metadata()?.len();
        let token = hex::encode(crate::keys::random::<16>()?.as_ref());
        let path = self.cfg.staging.join(&token);
        built.file.as_file().sync_all()?;

        self.catalog()
            .put(&format!("stage/{token}"), &(digest, built.defaults, size))?;
        if let Err(e) = built.file.persist_noclobber(&path) {
            self.catalog().delete(&format!("stage/{token}"))?;
            return Err(e.error.into());
        }
        Ok(ImageSource {
            manifest_digest: digest,
            staged_extent: token,
            physical_bytes: size,
        })
    }
    pub(crate) fn install(&self, source: &ImageSource) -> Result<ImageArtifact> {
        let id = image_id(source.manifest_digest);
        let token = &source.staged_extent;
        if token.is_empty() && self.catalog().defaults(&id)?.is_some() {
            let physical_bytes = self.pool.lookup(&id)?.iter().map(|e| e.sectors * 512).sum();
            return Ok(ImageArtifact {
                manifest_digest: source.manifest_digest,
                extent_id: id,
                physical_bytes,
            });
        }
        let stage_obj = format!("stage/{token}");
        let staged = if valid_token(token) {
            self.catalog()
                .get::<(Digest, crate::image::Defaults, u64)>(&stage_obj)?
        } else {
            None
        };
        if staged
            .as_ref()
            .is_some_and(|(digest, _, _)| *digest != source.manifest_digest)
        {
            return Err(Error::Fenced);
        }
        if valid_token(token) && self.pool.has_object(&stage_obj)? {
            let (_, defaults, _) = staged.ok_or(Error::Missing("staged image"))?;
            let promoted = self.pool.promote_staged_image(
                &stage_obj,
                &id,
                &defaults,
                source.physical_bytes,
            )?;
            if !promoted {
                self.pool.discard(&stage_obj)?;
            }
            let physical_bytes = self.pool.lookup(&id)?.iter().map(|e| e.sectors * 512).sum();
            return Ok(ImageArtifact {
                manifest_digest: source.manifest_digest,
                extent_id: id,
                physical_bytes,
            });
        }
        if self.catalog().defaults(&id)?.is_none() {
            if !valid_token(token) {
                return Err(Error::Invalid("staged image ID"));
            }
            let (_, defaults, _) = staged.ok_or(Error::Missing("staged image"))?;
            let mut image = std::fs::File::open(self.cfg.staging.join(token))?;
            let size = image.metadata()?.len();
            if size != source.physical_bytes
                || size == 0
                || !size.is_multiple_of(crate::extent::BLOCK_BYTES)
            {
                return Err(Error::Invalid("staged image length"));
            }
            let ext = self.pool.allocate(&id, size)?;
            let mut buffer = vec![0; 1 << 20];
            for e in ext {
                let mut pos = e.sector;
                while pos < e.sector + e.sectors {
                    let n = (e.sector + e.sectors - pos).min((buffer.len() / 512) as u64);
                    let data = &mut buffer[..n as usize * 512];
                    image.read_exact(data)?;
                    self.ctl
                        .lock()
                        .map_err(|_| Error::Interrupted)?
                        .blk_write(self.pool.disk(), pos, data)
                        .map_err(ctl_error)?;
                    pos += n;
                }
            }
            if image.stream_position()? != size {
                return Err(Error::Invalid("staged image length"));
            }
            self.ctl
                .lock()
                .map_err(|_| Error::Interrupted)?
                .blk_flush(self.pool.disk())
                .map_err(ctl_error)?;
            self.catalog().store_image(&id, &defaults)?;
        }
        if valid_token(token) {
            self.discard_staged_image(token)?;
        }
        let physical_bytes = self.pool.lookup(&id)?.iter().map(|e| e.sectors * 512).sum();
        Ok(ImageArtifact {
            manifest_digest: source.manifest_digest,
            extent_id: id,
            physical_bytes,
        })
    }
}
