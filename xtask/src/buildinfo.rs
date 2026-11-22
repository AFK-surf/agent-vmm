use object::{Object, ObjectSection, ObjectSegment, SegmentFlags};

const MAGIC: &[u8] = b"\xff Go buildinf:";

fn inline_string<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let mut size = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        if shift == 63 && byte > 1 {
            return None;
        }
        size |= u64::from(byte & 127) << shift;
        if byte < 128 {
            let (value, rest) = bytes.split_at_checked(size.try_into().ok()?)?;
            *bytes = rest;
            return Some(value);
        }
    }
    None
}

fn decode<'a>(header: &'a [u8], read: impl Fn(u64, u64) -> Option<&'a [u8]>) -> Option<&'a str> {
    let (header, mut data) = header.split_at_checked(32)?;
    let flags = header[15];
    let (version, modules) = if flags & 2 != 0 {
        (inline_string(&mut data)?, inline_string(&mut data)?)
    } else {
        let width = usize::from(header[14]);
        if !matches!(width, 4 | 8) {
            return None;
        }
        let ptr = |data: &[u8]| {
            let mut bytes = [0; 8];
            if flags & 1 == 0 {
                bytes[..width].copy_from_slice(&data[..width]);
                u64::from_le_bytes(bytes)
            } else {
                bytes[8 - width..].copy_from_slice(&data[..width]);
                u64::from_be_bytes(bytes)
            }
        };
        let string = |address: u64| {
            let words = read(address, (2 * width) as u64)?;
            read(ptr(words), ptr(&words[width..]))
        };
        (
            string(ptr(&header[16..]))?,
            string(ptr(&header[16 + width..]))?,
        )
    };
    if version.is_empty() || modules.len() < 33 || modules[modules.len() - 17] != b'\n' {
        return None;
    }
    std::str::from_utf8(&modules[16..modules.len() - 16]).ok()
}

pub fn read(bytes: &[u8]) -> Option<&str> {
    let object = object::File::parse(bytes).ok()?;
    let read = |address, size| {
        object
            .segments()
            .find_map(|s| s.data_range(address, size).ok().flatten())
    };
    let section = object
        .section_by_name(".go.buildinfo")
        .or_else(|| object.section_by_name("__go_buildinfo"));
    if let Some(section) = section {
        let data = section.data().ok()?;
        if data.starts_with(MAGIC) {
            return decode(data, read);
        }
    }
    // Stripped Go ELF files may carry only the writable load segment.
    for segment in object.segments() {
        let writable = match segment.flags() {
            SegmentFlags::Elf { p_flags } => {
                p_flags & (object::elf::PF_W | object::elf::PF_X) == object::elf::PF_W
            }
            SegmentFlags::MachO { initprot, .. } => initprot & object::macho::VM_PROT_WRITE != 0,
            _ => false,
        };
        if !writable {
            continue;
        }
        let data = segment.data().ok()?;
        let start = (segment.address().wrapping_neg() % 16) as usize;
        for offset in (start..data.len()).step_by(16) {
            if data[offset..].starts_with(MAGIC) {
                return decode(&data[offset..], read);
            }
        }
    }
    None
}

pub fn modules(info: &str) -> Vec<(&str, &str)> {
    let mut revision = None;
    let mut pending = Vec::new();
    let mut lines = info.lines().peekable();
    while let Some(line) = lines.next() {
        let mut fields = line.split('\t');
        match fields.next() {
            Some("build") => {
                if let Some(v) = fields.next().and_then(|s| s.strip_prefix("vcs.revision=")) {
                    revision = Some(v);
                }
            }
            Some(kind @ ("mod" | "dep")) => {
                let Some(mut path) = fields.next() else {
                    continue;
                };
                let mut version = fields.next().unwrap_or_default();
                let mut is_main = kind == "mod";
                let excluded = path.is_empty() || path == "github.com/AFK-surf/agent-vmm";
                if let Some(next) = lines.next_if(|line| line.starts_with("=>\t")) {
                    let mut replacement = next.split('\t').skip(1);
                    path = replacement.next().unwrap_or_default();
                    version = replacement.next().unwrap_or_default();
                    is_main = false;
                }
                if !excluded && !path.is_empty() && path != "github.com/AFK-surf/agent-vmm" {
                    pending.push((path, version, is_main));
                }
            }
            _ => {}
        }
    }
    pending
        .into_iter()
        .filter_map(|(path, mut version, main)| {
            if version.is_empty() || version == "(devel)" {
                version = revision.unwrap_or(version);
            }
            (!(main && (version.is_empty() || version == "(devel)"))).then_some((path, version))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_bazel_dependencies_and_development_versions() {
        assert_eq!(
            modules("dep\texample.org/dep\tv1.0.0\n"),
            [("example.org/dep", "v1.0.0")]
        );
        let dev = "path\texample.org/main/cmd\nmod\texample.org/main\t(devel)\t\n";
        assert!(modules(dev).is_empty());
        assert_eq!(
            modules(&format!("{dev}build\tvcs.revision=abc123\n")),
            [("example.org/main", "abc123")]
        );
        assert!(modules("mod\tgithub.com/AFK-surf/agent-vmm\tv2.0.0\n").is_empty());
    }

    #[test]
    fn legacy_headers_follow_target_size_and_byte_order() {
        let info = "dep\texample.org/mod\tv1\n";
        for width in [4usize, 8] {
            for big in [false, true] {
                let put = |dst: &mut [u8], n: u64| {
                    if big {
                        dst[..width].copy_from_slice(&n.to_be_bytes()[8 - width..]);
                    } else {
                        dst[..width].copy_from_slice(&n.to_le_bytes()[..width]);
                    }
                };
                let mut memory = [0u8; 256];
                memory[..14].copy_from_slice(MAGIC);
                memory[14] = width as u8;
                memory[15] = u8::from(big);
                put(&mut memory[16..], 0x1000 + 32);
                put(&mut memory[16 + width..], 0x1000 + 48);
                put(&mut memory[32..], 0x1000 + 64);
                put(&mut memory[32 + width..], 6);
                memory[64..70].copy_from_slice(b"go1.17");
                put(&mut memory[48..], 0x1000 + 80);
                put(&mut memory[48 + width..], 32 + info.len() as u64);
                memory[96..96 + info.len()].copy_from_slice(info.as_bytes());
                let read = |address: u64, size: u64| {
                    let offset = address.checked_sub(0x1000)? as usize;
                    memory.get(offset..offset.checked_add(size as usize)?)
                };
                assert_eq!(decode(&memory[..32], read), Some(info));
            }
        }
    }
}
