use rustix::fs::{AtFlags, Dir, Mode, OFlags, mkdirat, openat, renameat, unlinkat};
use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs::{File, Metadata},
    io::{self, Read},
    os::unix::{
        ffi::OsStrExt,
        fs::{FileExt, MetadataExt},
    },
    sync::Arc,
};

const MAX: u32 = 1 << 20;
const RD: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);
const DIRECTORY: OFlags = RD.union(OFlags::DIRECTORY);
fn err(n: i32) -> io::Error {
    io::Error::from_raw_os_error(n)
}
type R<T> = io::Result<T>;

struct Message<'a>(&'a [u8]);
impl<'a> Message<'a> {
    fn num<const N: usize>(&mut self) -> R<[u8; N]> {
        let mut b = [0; N];
        self.0.read_exact(&mut b)?;
        Ok(b)
    }
    fn u16(&mut self) -> R<u16> {
        Ok(u16::from_le_bytes(self.num()?))
    }
    fn u32(&mut self) -> R<u32> {
        Ok(u32::from_le_bytes(self.num()?))
    }
    fn u64(&mut self) -> R<u64> {
        Ok(u64::from_le_bytes(self.num()?))
    }
    fn string(&mut self) -> R<&'a [u8]> {
        let n = self.u16()? as usize;
        let (s, rest) = self.0.split_at_checked(n).ok_or(err(22))?;
        self.0 = rest;
        Ok(s)
    }
    fn name(&mut self) -> R<OsString> {
        let s = self.string()?;
        if s.is_empty() || s == b".." || s == b"." || s.contains(&b'/') || s.contains(&0) {
            return Err(err(22));
        }
        Ok(OsStr::from_bytes(s).into())
    }
}

fn qid(md: &Metadata) -> Vec<u8> {
    let mut out = vec![if md.is_dir() { 0x80 } else { 0 }];
    out.extend(0u32.to_le_bytes());
    out.extend(md.ino().to_le_bytes());
    out
}
fn string(out: &mut Vec<u8>, s: &[u8]) {
    out.extend((s.len() as u16).to_le_bytes());
    out.extend(s);
}
fn check_regular(file: File) -> R<File> {
    let m = file.metadata()?;
    if m.is_dir() || m.is_file() {
        Ok(file)
    } else {
        Err(err(13))
    }
}

struct Fid {
    path: Vec<OsString>,
    file: Option<File>,
}
pub(super) struct Session {
    root: Arc<File>,
    share: String,
    ro: bool,
    fids: BTreeMap<u32, Fid>,
    pub msize: u32,
}
impl Session {
    pub fn new(root: Arc<File>, share: String, ro: bool) -> Self {
        Self {
            root,
            share,
            ro,
            fids: BTreeMap::new(),
            msize: MAX,
        }
    }
    pub fn process(&mut self, frame: &[u8]) -> Vec<u8> {
        let op = frame[0];
        let tag = &frame[1..3];
        let (kind, body) = match self.dispatch(op, Message(&frame[3..])) {
            Ok(data) => (op + 1, data),
            Err(e) => (
                7,
                (e.raw_os_error().unwrap_or(5) as u32)
                    .to_le_bytes()
                    .to_vec(),
            ),
        };
        let mut out = ((body.len() + 7) as u32).to_le_bytes().to_vec();
        out.push(kind);
        out.extend(tag);
        out.extend(body);
        out
    }

    fn open(&self, path: &[OsString], flags: OFlags) -> R<File> {
        let mut base = self.root.try_clone()?;
        let Some((last, parents)) = path.split_last() else {
            return Ok(base);
        };
        for item in parents {
            base = openat(&base, item.as_os_str(), DIRECTORY, Mode::empty())?.into();
        }
        check_regular(
            openat(
                &base,
                last.as_os_str(),
                flags | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )?
            .into(),
        )
    }
    fn fid(&self, id: u32) -> R<&Fid> {
        self.fids.get(&id).ok_or(err(9))
    }
    fn writable(&self) -> R<()> {
        if self.ro { Err(err(30)) } else { Ok(()) }
    }
    fn dispatch(&mut self, op: u8, mut m: Message<'_>) -> R<Vec<u8>> {
        let mut out = Vec::new();
        match op {
            100 => {
                let n = m.u32()?.min(MAX);
                if n < 256 {
                    return Err(err(22));
                }
                self.msize = n;
                self.fids.clear();
                out.extend(n.to_le_bytes());
                string(
                    &mut out,
                    if m.string()? == b"9P2000.L" {
                        b"9P2000.L"
                    } else {
                        b"unknown"
                    },
                );
            }
            104 => {
                let fid = m.u32()?;
                m.u32()?;
                m.string()?;
                if m.string()? != self.share.as_bytes() {
                    return Err(err(13));
                }
                m.u32()?;
                if self.fids.len() >= 4096 {
                    return Err(err(24));
                }
                if self.fids.contains_key(&fid) {
                    return Err(err(17));
                }
                out = qid(&self.root.metadata()?);
                self.fids.insert(
                    fid,
                    Fid {
                        path: Vec::new(),
                        file: None,
                    },
                );
            }
            108 => {
                m.u16()?;
            }
            110 => {
                let fid = m.u32()?;
                let new = m.u32()?;
                let count = m.u16()?;
                let mut path = self.fid(fid)?.path.clone();
                if count > 16 || (new != fid && self.fids.contains_key(&new)) {
                    return Err(err(22));
                }
                let mut walked = Vec::new();
                for _ in 0..count {
                    path.push(m.name()?);
                    match self.open(&path, RD) {
                        Ok(f) => walked.extend(qid(&f.metadata()?)),
                        Err(e) if walked.is_empty() => return Err(e),
                        Err(_) => break,
                    }
                }
                out.extend(((walked.len() / 13) as u16).to_le_bytes());
                out.extend(&walked);
                if walked.len() == usize::from(count) * 13 {
                    if self.fids.len() >= 4096 && !self.fids.contains_key(&new) {
                        return Err(err(24));
                    }
                    self.fids.insert(new, Fid { path, file: None });
                }
            }
            12 => {
                let id = m.u32()?;
                let bits = m.u32()?;
                let mut flags = match bits & 3 {
                    0 => OFlags::RDONLY,
                    1 => OFlags::WRONLY,
                    2 => OFlags::RDWR,
                    _ => return Err(err(22)),
                };
                if bits & 3 != 0 || bits & 0x200 != 0 {
                    self.writable()?;
                }
                if bits & 0x200 != 0 {
                    flags |= OFlags::TRUNC;
                }
                let f = self.open(&self.fid(id)?.path, flags)?;
                out = qid(&f.metadata()?);
                out.extend(0u32.to_le_bytes());
                self.fids.get_mut(&id).unwrap().file = Some(f);
            }
            14 => {
                self.writable()?;
                let id = m.u32()?;
                let name = m.name()?;
                let flags = m.u32()?;
                let mode = m.u32()?;
                m.u32()?;
                let mut path = self.fid(id)?.path.clone();
                let dir = self.open(&path, DIRECTORY)?;
                let access = match flags & 3 {
                    0 => OFlags::RDONLY,
                    1 => OFlags::WRONLY,
                    2 => OFlags::RDWR,
                    _ => return Err(err(22)),
                };
                let f: File = openat(
                    &dir,
                    &*name,
                    OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC | access,
                    Mode::from_bits_truncate((mode & 0o777) as _),
                )?
                .into();
                out = qid(&f.metadata()?);
                out.extend(0u32.to_le_bytes());
                path.push(name);
                self.fids.insert(
                    id,
                    Fid {
                        path,
                        file: Some(f),
                    },
                );
            }
            24 => {
                let id = m.u32()?;
                m.u64()?;
                let md = self.open(&self.fid(id)?.path, RD)?.metadata()?;
                out.extend(0x7ffu64.to_le_bytes());
                out.extend(qid(&md));
                for n in [md.mode(), md.uid(), md.gid()] {
                    out.extend(n.to_le_bytes());
                }
                for n in [
                    md.nlink(),
                    md.rdev(),
                    md.size(),
                    md.blksize(),
                    md.blocks(),
                    md.atime() as u64,
                    md.atime_nsec() as u64,
                    md.mtime() as u64,
                    md.mtime_nsec() as u64,
                    md.ctime() as u64,
                    md.ctime_nsec() as u64,
                    0,
                    0,
                    0,
                    0,
                ] {
                    out.extend(n.to_le_bytes());
                }
            }
            116 | 118 => {
                let id = m.u32()?;
                let off = m.u64()?;
                let n = m.u32()?.min(self.msize - 11) as usize;
                let f = self.fid(id)?.file.as_ref().ok_or(err(9))?;
                if op == 116 {
                    let mut data = vec![0; n];
                    let n = f.read_at(&mut data, off)?;
                    out.extend((n as u32).to_le_bytes());
                    out.extend(&data[..n]);
                } else {
                    self.writable()?;
                    if m.0.len() != n {
                        return Err(err(22));
                    }
                    out.extend((f.write_at(m.0, off)? as u32).to_le_bytes());
                }
            }
            40 => {
                let id = m.u32()?;
                let off = m.u64()?;
                let n = m.u32()?.min(self.msize - 11) as usize;
                let dir = self.open(&self.fid(id)?.path, DIRECTORY)?;
                let mut data = Vec::new();
                for (i, entry) in Dir::read_from(&dir)?.enumerate() {
                    if (i as u64) < off {
                        continue;
                    }
                    let entry = entry?;
                    let name = entry.file_name().to_bytes();
                    if name == b"." || name == b".." {
                        continue;
                    }
                    let Ok(f) = openat(&dir, OsStr::from_bytes(name), RD, Mode::empty()) else {
                        continue;
                    };
                    let md = File::from(f).metadata()?;
                    if !md.is_dir() && !md.is_file() {
                        continue;
                    }
                    let mut item = qid(&md);
                    item.extend((i as u64 + 1).to_le_bytes());
                    item.push(if md.is_dir() { 4 } else { 8 });
                    string(&mut item, name);
                    if data.len() + item.len() > n {
                        break;
                    }
                    data.extend(item);
                }
                out.extend((data.len() as u32).to_le_bytes());
                out.extend(data);
            }
            8 => {
                let file = self.open(&self.fid(m.u32()?)?.path, RD)?;
                let stat = rustix::fs::fstatvfs(&file)?;
                out.extend(0x01021997_u32.to_le_bytes());
                out.extend((stat.f_bsize as u32).to_le_bytes());
                for value in [
                    stat.f_blocks,
                    stat.f_bfree,
                    stat.f_bavail,
                    stat.f_files,
                    stat.f_ffree,
                    0,
                ] {
                    out.extend(value.to_le_bytes());
                }
                out.extend((stat.f_namemax as u32).to_le_bytes());
            }
            50 => {
                let id = m.u32()?;
                self.fid(id)?.file.as_ref().ok_or(err(9))?.sync_all()?;
            }
            72 => {
                self.writable()?;
                let id = m.u32()?;
                let name = m.name()?;
                let mode = m.u32()?;
                m.u32()?;
                let dir = self.open(&self.fid(id)?.path, DIRECTORY)?;
                mkdirat(&dir, &*name, Mode::from_bits_truncate((mode & 0o777) as _))?;
                let f: File = openat(&dir, &*name, DIRECTORY, Mode::empty())?.into();
                out = qid(&f.metadata()?);
            }
            74 => {
                self.writable()?;
                let old = self.open(&self.fid(m.u32()?)?.path, DIRECTORY)?;
                let a = m.name()?;
                let new = self.open(&self.fid(m.u32()?)?.path, DIRECTORY)?;
                let b = m.name()?;
                renameat(old, &*a, new, &*b)?;
            }
            76 => {
                self.writable()?;
                let dir = self.open(&self.fid(m.u32()?)?.path, DIRECTORY)?;
                let name = m.name()?;
                let flags = match m.u32()? {
                    0 => AtFlags::empty(),
                    0x200 => AtFlags::REMOVEDIR,
                    _ => return Err(err(22)),
                };
                unlinkat(dir, &*name, flags)?;
            }
            120 => {
                if self.fids.remove(&m.u32()?).is_none() {
                    return Err(err(9));
                }
            }
            _ => return Err(err(95)),
        }
        Ok(out)
    }
}
