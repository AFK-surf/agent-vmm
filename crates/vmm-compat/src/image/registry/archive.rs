use super::{http, interrupted, invalid, too_large};
use crate::image::MAX_ARCHIVE_BYTES;
use reqwest::Response;
use std::path::Path;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tonic::Status;

pub(super) struct Archive {
    tmp: NamedTempFile,
    file: tokio::fs::File,
    remaining: u64,
}

fn io_error(error: std::io::Error) -> Status {
    if error.kind() == std::io::ErrorKind::StorageFull {
        Status::resource_exhausted("image staging filesystem full")
    } else {
        Status::unavailable("image staging write failed")
    }
}

impl Archive {
    pub fn new(dir: &Path) -> Result<Self, Status> {
        let tmp = NamedTempFile::new_in(dir).map_err(io_error)?;
        let file = tokio::fs::File::from_std(tmp.reopen().map_err(io_error)?);
        Ok(Self {
            tmp,
            file,
            remaining: MAX_ARCHIVE_BYTES - 1024,
        })
    }

    async fn start(&mut self, name: &str, size: u64) -> Result<(), Status> {
        let padded = size.checked_add(511).ok_or_else(too_large)? / 512 * 512;
        self.remaining = self
            .remaining
            .checked_sub(512)
            .and_then(|n| n.checked_sub(padded))
            .ok_or_else(too_large)?;
        let mut header = tar::Header::new_ustar();
        header.set_path(name).map_err(|_| invalid())?;
        header.set_size(size);
        header.set_mode(0o600);
        header.set_cksum();
        self.file
            .write_all(header.as_bytes())
            .await
            .map_err(io_error)
    }

    async fn pad(&mut self, size: u64) -> Result<(), Status> {
        let count = (512 - size % 512) % 512;
        self.file
            .write_all(&[0; 512][..count as usize])
            .await
            .map_err(io_error)
    }

    pub async fn bytes(&mut self, name: &str, data: &[u8]) -> Result<(), Status> {
        let size = data.len() as u64;
        self.start(name, size).await?;
        self.file.write_all(data).await.map_err(io_error)?;
        self.pad(size).await
    }

    pub async fn layer(
        &mut self,
        name: &str,
        size: u64,
        mut response: Response,
    ) -> Result<(), Status> {
        self.start(name, size).await?;
        let mut left = size;
        while let Some(bytes) = response.chunk().await.map_err(http::network)? {
            left = left
                .checked_sub(bytes.len() as u64)
                .ok_or_else(interrupted)?;
            self.file.write_all(&bytes).await.map_err(io_error)?;
        }
        if left != 0 {
            return Err(interrupted());
        }
        self.pad(size).await
    }

    pub async fn finish(mut self) -> Result<NamedTempFile, Status> {
        self.file.write_all(&[0; 1024]).await.map_err(io_error)?;
        self.file.flush().await.map_err(io_error)?;
        Ok(self.tmp)
    }
}
