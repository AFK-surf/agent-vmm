use crate::{backend::LocalDom0Backend, catalog::image_id, extent::ctl_error};
use std::{
    io::{ErrorKind, Read, Seek},
    path::Path,
};
use vmm_core::{Digest, Error, ImageArtifact, ImageSource, Result};

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
    pub fn discard_staged_image(&self, token: &str) -> Result<()> {
        if !valid_token(token) {
            return Err(Error::Invalid("staged image ID"));
        }
        match std::fs::remove_file(self.cfg.staging.join(token)) {
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            result => result?,
        }
        self.catalog().delete(&format!("stage/{token}"))
    }
    pub fn image_installed(&self, digest: Digest) -> Result<bool> {
        Ok(self.catalog().defaults(&image_id(digest))?.is_some())
    }

    /// Build a staged EROFS file and publish its activation parameters alongside it.
    pub fn stage_image(&self, source: &Path, digest: Digest) -> Result<ImageSource> {
        let built = crate::image::build(source, digest, &self.cfg.staging)?;
        self.publish_stage(built, digest)
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
            .put(&format!("stage/{token}"), &(digest, built.defaults))?;
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
        let staged = if valid_token(token) {
            self.catalog()
                .get::<(Digest, crate::image::Defaults)>(&format!("stage/{token}"))?
        } else {
            None
        };
        if staged
            .as_ref()
            .is_some_and(|(digest, _)| *digest != source.manifest_digest)
        {
            return Err(Error::Fenced);
        }
        if self.catalog().defaults(&id)?.is_none() {
            if !valid_token(token) {
                return Err(Error::Invalid("staged image ID"));
            }
            let (_, defaults) = staged.ok_or(Error::Missing("staged image"))?;
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
