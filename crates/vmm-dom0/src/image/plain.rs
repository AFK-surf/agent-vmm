//! Uncompressed EROFS fallback. The metadata block starts at block one.
//! Each extended inode occupies two 32-byte NID slots. Data uses flat blocks.
use super::layers::{Inode, Tree};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use vmm_core::{Error, Result};

const BLOCK: u64 = 4096;
fn u32_value(n: u64) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::Invalid("EROFS field overflow"))
}

type DirEntry = (Vec<u8>, u64, u8);
fn directory(mut entries: Vec<DirEntry>) -> Result<Vec<u8>> {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut bytes = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = start;
        let mut needed = 0;
        while let Some((name, _, _)) = entries.get(end) {
            if name.is_empty() || name.len() > 255 {
                return Err(Error::Invalid("EROFS filename"));
            }
            if needed + 12 + name.len() > BLOCK as usize {
                break;
            }
            needed += 12 + name.len();
            end += 1;
        }
        let mut offset = 12 * (end - start);
        for (name, nid, kind) in &entries[start..end] {
            bytes.extend(nid.to_le_bytes());
            bytes.extend((offset as u16).to_le_bytes());
            bytes.extend([*kind, 0]);
            offset += name.len();
        }
        for (name, _, _) in &entries[start..end] {
            bytes.extend(name);
        }
        if end < entries.len() {
            bytes.resize(bytes.len().next_multiple_of(BLOCK as usize), 0);
        }
        start = end;
    }
    Ok(bytes)
}
fn dir_inode() -> Arc<Inode> {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_mode(0o755);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(0);
    Arc::new(Inode {
        header,
        data: 0,
        link: None,
    })
}

pub(super) fn write(tree: &mut Tree, out: &mut File) -> Result<()> {
    let needed: Vec<_> = tree
        .nodes
        .keys()
        .flat_map(|p| p.ancestors().skip(1).map(Path::to_owned))
        .collect();
    for path in needed.into_iter().chain([PathBuf::new()]) {
        tree.nodes.entry(path).or_insert_with(dir_inode);
    }
    let mut indices = BTreeMap::new();
    let mut by_path = BTreeMap::new();
    let mut inodes = Vec::new();
    let mut nlinks = Vec::new();
    for (path, inode) in &tree.nodes {
        let index = *indices.entry(Arc::as_ptr(inode)).or_insert_with(|| {
            let n = inodes.len();
            inodes.push(inode);
            nlinks.push(0_u32);
            n
        });
        nlinks[index] += 1;
        by_path.insert(path.as_path(), index);
    }
    let mut children = BTreeMap::<usize, Vec<DirEntry>>::new();
    for (path, &index) in &by_path {
        let dir = inodes[index].header.entry_type().is_dir();
        let parent = *by_path
            .get(path.parent().unwrap_or(Path::new("")))
            .ok_or(Error::Invalid("image parent"))?;
        if dir {
            nlinks[index] += 1;
            children.entry(index).or_default().extend([
                (b".".to_vec(), index as u64 * 2, 2),
                (b"..".to_vec(), parent as u64 * 2, 2),
            ]);
        }
        if let Some(name) = path.file_name() {
            if dir {
                nlinks[parent] += 1;
            }
            let kind = if dir {
                2
            } else if inodes[index].header.entry_type().is_symlink() {
                7
            } else {
                1
            };
            children.entry(parent).or_default().push((
                name.as_encoded_bytes().to_vec(),
                index as u64 * 2,
                kind,
            ));
        }
    }
    let mut block = 1 + (inodes.len() as u64 * 64).div_ceil(BLOCK);
    for (index, node) in inodes.iter().enumerate() {
        let header = &node.header;
        let kind = header.entry_type();
        out.seek(SeekFrom::Start(block * BLOCK))?;
        let (size, mode) = if kind.is_dir() {
            let data = directory(
                children
                    .remove(&index)
                    .ok_or(Error::Invalid("directory inode"))?,
            )?;
            out.write_all(&data)?;
            (data.len() as u64, 0o040000)
        } else if kind.is_symlink() {
            let target = node
                .link
                .as_ref()
                .ok_or(Error::Invalid("symlink target"))?
                .as_os_str()
                .as_encoded_bytes();
            out.write_all(target)?;
            (target.len() as u64, 0o120000)
        } else {
            tree.file.seek(SeekFrom::Start(node.data))?;
            let size = header.size()?;
            if std::io::copy(&mut (&mut tree.file).take(size), out)? != size {
                return Err(Error::Invalid("short image file"));
            }
            (size, 0o100000)
        };
        let mut inode = [0; 64];
        inode[0] = 1;
        let mode = mode | (header.mode()? & 0o7777) as u16;
        inode[4..6].copy_from_slice(&mode.to_le_bytes());
        inode[8..16].copy_from_slice(&size.to_le_bytes());
        inode[16..20].copy_from_slice(&u32_value(block)?.to_le_bytes());
        inode[20..24].copy_from_slice(&u32_value(index as u64 + 1)?.to_le_bytes());
        inode[24..28].copy_from_slice(&u32_value(header.uid()?)?.to_le_bytes());
        inode[28..32].copy_from_slice(&u32_value(header.gid()?)?.to_le_bytes());
        inode[32..40].copy_from_slice(&header.mtime()?.to_le_bytes());
        inode[44..48].copy_from_slice(&nlinks[index].to_le_bytes());
        out.seek(SeekFrom::Start(BLOCK + index as u64 * 64))?;
        out.write_all(&inode)?;
        block += size.div_ceil(BLOCK);
    }
    let mut sb = [0; 128];
    sb[..4].copy_from_slice(&0xe0f5e1e2_u32.to_le_bytes());
    sb[12] = 12;
    sb[16..24].copy_from_slice(&(inodes.len() as u64).to_le_bytes());
    sb[36..40].copy_from_slice(&u32_value(block)?.to_le_bytes());
    sb[40] = 1;
    out.seek(SeekFrom::Start(1024))?;
    out.write_all(&sb)?;
    out.set_len(block * BLOCK)?;
    Ok(())
}
