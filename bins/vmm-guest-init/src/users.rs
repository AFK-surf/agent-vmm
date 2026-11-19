//! Resolves a workload user from the `passwd` and `group` files of the image.

use std::fs;
use std::path::Path;

const MAX_DATABASE_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
    pub home: Option<String>,
}

fn database(etc: &Path, name: &str) -> Vec<Vec<String>> {
    let path = etc.join(name);
    match fs::metadata(&path) {
        Ok(metadata) if metadata.is_file() && metadata.len() <= MAX_DATABASE_BYTES => {}
        _ => return Vec::new(),
    }
    let text = fs::read(&path).unwrap_or_default();
    String::from_utf8_lossy(&text)
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| line.split(':').map(str::to_owned).collect())
        .collect()
}

/// Resolves `name`, `uid`, `name:group`, or `uid:gid`.
///
/// A numeric user without a `passwd` entry gets group 0. An explicit group
/// replaces the user's supplementary groups.
pub fn resolve(etc: &Path, user: &str) -> Result<Credentials, String> {
    let (user, group) = match user.split_once(':') {
        Some((user, group)) => (user, Some(group)),
        None => (user, None),
    };
    let passwd = database(etc, "passwd");
    let entry = passwd.iter().find(|fields| {
        fields.len() >= 4
            && (fields[0] == user
                || user
                    .parse::<u32>()
                    .is_ok_and(|uid| fields[2].parse() == Ok(uid)))
    });
    let (uid, mut gid, name, home) = match (entry, user.parse::<u32>()) {
        (Some(fields), _) => {
            let uid = fields[2]
                .parse()
                .map_err(|_| format!("passwd entry of {user:?}"))?;
            let gid = fields[3]
                .parse()
                .map_err(|_| format!("passwd entry of {user:?}"))?;
            (
                uid,
                gid,
                Some(fields[0].as_str()),
                fields.get(5).filter(|home| !home.is_empty()),
            )
        }
        (None, Ok(uid)) => (uid, 0, None, None),
        (None, Err(_)) => return Err(format!("user {user:?} is not in the image")),
    };
    let groups_file = database(etc, "group");
    let mut groups = Vec::new();
    match group {
        Some(group) => {
            gid = match group.parse::<u32>() {
                Ok(gid) => gid,
                Err(_) => groups_file
                    .iter()
                    .find(|fields| fields.len() >= 3 && fields[0] == group)
                    .and_then(|fields| fields[2].parse().ok())
                    .ok_or_else(|| format!("group {group:?} is not in the image"))?,
            };
        }
        None => {
            if let Some(name) = name {
                groups = groups_file
                    .iter()
                    .filter(|fields| {
                        fields.len() >= 4 && fields[3].split(',').any(|member| member == name)
                    })
                    .filter_map(|fields| fields[2].parse().ok())
                    .collect();
            }
        }
    }
    if !groups.contains(&gid) {
        groups.insert(0, gid);
    }
    Ok(Credentials {
        uid,
        gid,
        groups,
        home: home.cloned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_names_numbers_and_explicit_groups() {
        let etc = tempfile::tempdir().unwrap();
        fs::write(
            etc.path().join("passwd"),
            "root:x:0:0:root:/root:/bin/sh\napp:x:1000:1000::/home/app:/bin/sh\n",
        )
        .unwrap();
        fs::write(
            etc.path().join("group"),
            "app:x:1000:\ndocker:x:998:app,other\nwheel:x:10:root\n",
        )
        .unwrap();

        let app = resolve(etc.path(), "app").unwrap();
        assert_eq!((app.uid, app.gid), (1000, 1000));
        assert_eq!(app.groups, [1000, 998]);
        assert_eq!(app.home.as_deref(), Some("/home/app"));
        assert_eq!(resolve(etc.path(), "1000").unwrap(), app);

        let explicit = resolve(etc.path(), "app:wheel").unwrap();
        assert_eq!(
            (explicit.uid, explicit.gid, explicit.groups),
            (1000, 10, vec![10])
        );
        let numeric = resolve(etc.path(), "4242:7").unwrap();
        assert_eq!((numeric.uid, numeric.gid, numeric.home), (4242, 7, None));
        assert_eq!(resolve(etc.path(), "4242").unwrap().gid, 0);

        assert!(resolve(etc.path(), "missing").is_err());
        assert!(resolve(etc.path(), "app:missing").is_err());
    }
}
