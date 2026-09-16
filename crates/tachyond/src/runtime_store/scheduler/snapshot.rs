//! Local snapshots, not isolation. Recovery requires the exact host descriptor.
//! Roots/ancestors must be host-controlled; adversarial same-UID mutation and
//! mount changes are not an isolation boundary. Detached descriptors never
//! follow replacement symlinks; path association is rechecked before success.
#![forbid(unsafe_code)]
use rustix::fs::{open, openat, Mode, OFlags};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};
use tachyon_api::campaign::ChildProfile;

#[cfg(test)]
thread_local! {
    pub(in crate::runtime_store) static CRASH: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
    static HOOK: std::cell::RefCell<Option<(&'static str, Box<dyn FnOnce()>)>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(in crate::runtime_store) fn checkpoint(name: &str) -> Result<(), String> {
    let hook = HOOK.with(|hook| {
        if hook
            .borrow()
            .as_ref()
            .is_some_and(|(point, _)| *point == name)
        {
            hook.borrow_mut().take()
        } else {
            None
        }
    });
    if let Some((_, hook)) = hook {
        hook();
    }
    CRASH.with(|crash| {
        if crash.get() == Some(name) {
            crash.set(None);
            Err(format!("injected crash: {name}"))
        } else {
            Ok(())
        }
    })
}

#[derive(Default)]
pub(super) struct Prepared;

// Retain failed preparation in place. Never recursively delete a directory that
// another same-user process could have changed since preparation started.

fn open_root(path: &Path) -> Result<File, String> {
    if !path.is_absolute() {
        return Err("snapshot root must be absolute".into());
    }
    let mut root = File::from(
        open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| e.to_string())?,
    );
    for component in path
        .strip_prefix("/")
        .map_err(|e| e.to_string())?
        .components()
    {
        let Component::Normal(name) = component else {
            return Err("snapshot root must be canonical without traversal".into());
        };
        root = File::from(
            openat(
                &root,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|e| e.to_string())?,
        );
    }
    Ok(root)
}

fn same_object(a: &File, b: &File) -> Result<bool, String> {
    let a = a.metadata().map_err(|e| e.to_string())?;
    let b = b.metadata().map_err(|e| e.to_string())?;
    Ok((a.dev(), a.ino()) == (b.dev(), b.ino()))
}

fn associated(root: &File, name: &Path, pinned: &File) -> Result<(), String> {
    if !same_object(&open_relative(root, name)?, pinned)? {
        return Err("snapshot identity changed; retained for operator review".into());
    }
    Ok(())
}

fn root_associated(path: &Path, pinned: &File) -> Result<(), String> {
    if !same_object(&open_root(path)?, pinned)? {
        return Err("managed root identity changed; retained for operator review".into());
    }
    Ok(())
}

fn exists(root: &File, name: &Path) -> Result<bool, String> {
    match rustix::fs::statat(root, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => Ok(true),
        Err(rustix::io::Errno::NOENT) => Ok(false),
        Err(e) => Err(e.to_string()),
    }
}

fn create_file(root: &File, name: &Path, mode: Mode) -> Result<File, String> {
    if name.components().count() != 1
        || !matches!(name.components().next(), Some(Component::Normal(_)))
    {
        return Err("invalid snapshot file name".into());
    }
    openat(
        root,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        mode,
    )
    .map(File::from)
    .map_err(|e| e.to_string())
}

fn create_directory(root: &File, name: &Path) -> Result<File, String> {
    rustix::fs::mkdirat(root, name, Mode::from_raw_mode(0o700)).map_err(|e| e.to_string())?;
    openat(
        root,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|e| e.to_string())
}

// renameat2 cannot condition a rename on an inode. Private host-controlled parents
// are required. Check both sides of the syscall; on mismatch never delete/rollback.
fn move_snapshot(root: &File, source: &Path, target: &Path, pinned: &File) -> Result<(), String> {
    associated(root, source, pinned)?;
    #[cfg(test)]
    checkpoint("rename")?;
    rustix::fs::renameat_with(
        root,
        source,
        root,
        target,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|e| e.to_string())?;
    root.sync_all().map_err(|e| e.to_string())?;
    associated(root, target, pinned).map_err(|e| {
        format!("rename identity mismatch; moved entry retained without deletion or rollback: {e}")
    })
}

fn open_relative(root: &File, path: &Path) -> Result<File, String> {
    let mut fd = rustix::io::dup(root).map_err(|e| e.to_string())?;
    let mut parts = path.components().peekable();
    while let Some(part) = parts.next() {
        let Component::Normal(name) = part else {
            return Err("invalid snapshot path".into());
        };
        let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        fd = openat(
            &fd,
            name,
            if parts.peek().is_some() {
                flags | OFlags::DIRECTORY
            } else {
                flags
            },
            Mode::empty(),
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(File::from(fd))
}

fn read_regular(root: &File, path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let file = open_relative(root, path)?;
    read_opened(&file, limit)
}

fn read_opened(file: &File, limit: u64) -> Result<Vec<u8>, String> {
    if !file.metadata().map_err(|e| e.to_string())?.is_file() {
        return Err("snapshot is not a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > limit {
        return Err("snapshot size limit exceeded".into());
    }
    Ok(bytes)
}

fn verify(
    profile: &ChildProfile,
    root: &File,
    checkpoint: &serde_json::Value,
    complete: bool,
) -> Result<(), String> {
    #[cfg(test)]
    self::checkpoint("verify")?;
    let mut expected = std::collections::BTreeSet::from([
        PathBuf::from("work"),
        PathBuf::from("home"),
        PathBuf::from("owned.json"),
        PathBuf::from("prepared.json"),
    ]);
    let mut directories =
        std::collections::BTreeSet::from([PathBuf::from("work"), PathBuf::from("home")]);
    let mut bytes = 0u64;
    let mut files = 0usize;
    for (index, input) in profile.inputs.iter().enumerate() {
        for approved in &input.files {
            files += 1;
            if files > profile.max_input_files {
                return Err("snapshot file quota exceeded".into());
            }
            let relative = PathBuf::from("work/inputs")
                .join(index.to_string())
                .join(&approved.path);
            let mut parent = relative.parent();
            while let Some(path) = parent.filter(|p| !p.as_os_str().is_empty()) {
                expected.insert(path.to_owned());
                directories.insert(path.to_owned());
                parent = path.parent();
            }
            expected.insert(relative);
        }
    }
    // Enumerate before reading: reject symlinks, special files and extra entries,
    // including files in home. Never traverse a symlink during reconciliation.
    let mut pending = vec![(PathBuf::new(), root.try_clone().map_err(|e| e.to_string())?)];
    let mut seen = std::collections::BTreeSet::new();
    let mut inventory = std::collections::BTreeMap::new();
    while let Some((relative, dir)) = pending.pop() {
        if !dir.metadata().map_err(|e| e.to_string())?.is_dir() {
            return Err("snapshot directory replaced".into());
        }
        for entry in rustix::fs::Dir::read_from(&dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            if matches!(entry.file_name().to_bytes(), b"." | b"..") {
                continue;
            }
            use std::os::unix::ffi::OsStrExt;
            let name = Path::new(std::ffi::OsStr::from_bytes(entry.file_name().to_bytes()));
            let path = relative.join(name);
            if !expected.contains(&path) {
                return Err("snapshot has unexpected entry; retained unchanged".into());
            }
            let opened = open_relative(&dir, name)?;
            let kind = opened.metadata().map_err(|e| e.to_string())?;
            if if directories.contains(&path) {
                !kind.is_dir()
            } else {
                !kind.is_file()
            } {
                return Err("snapshot has unexpected entry; retained unchanged".into());
            }
            if kind.is_dir() {
                pending.push((path.clone(), opened.try_clone().map_err(|e| e.to_string())?));
            }
            seen.insert(path.clone());
            inventory.insert(path, opened);
        }
    }
    if complete && seen != expected {
        return Err("snapshot incomplete; retained unchanged".into());
    }
    for name in ["owned.json", "prepared.json"] {
        if !complete && name == "prepared.json" && !seen.contains(Path::new(name)) {
            continue;
        }
        let actual: serde_json::Value = serde_json::from_slice(&read_opened(
            inventory
                .get(Path::new(name))
                .ok_or("missing ownership marker")?,
            262144,
        )?)
        .map_err(|e| e.to_string())?;
        if actual != *checkpoint {
            return Err("snapshot ownership/approval mismatch; retained unchanged".into());
        }
    }
    if complete {
        for (index, input) in profile.inputs.iter().enumerate() {
            for approved in &input.files {
                let data = read_opened(
                    inventory
                        .get(
                            &PathBuf::from("work/inputs")
                                .join(index.to_string())
                                .join(&approved.path),
                        )
                        .ok_or("missing snapshot input")?,
                    profile
                        .max_input_bytes
                        .checked_sub(bytes)
                        .ok_or("snapshot byte quota exceeded")?,
                )?;
                bytes += data.len() as u64;
                if format!("{:x}", Sha256::digest(&data)) != approved.sha256 {
                    return Err("snapshot SHA-256 mismatch; retained unchanged".into());
                }
            }
        }
    }
    for (path, file) in inventory {
        associated(root, &path, &file)?;
    }
    Ok(())
}

pub(crate) fn validate_roots(profile: &ChildProfile, protected: &Path) -> Result<(), String> {
    for path in std::iter::once(&profile.managed_root).chain(profile.inputs.iter().map(|i| &i.root))
    {
        open_root(path)?;
        if !path.is_absolute()
            || path.starts_with(protected)
            || protected.starts_with(path)
            || std::env::var_os("HOME").is_some_and(|h| PathBuf::from(h).starts_with(path))
        {
            return Err("dynamic roots must be canonical, dedicated and outside daemon storage and broad HOME roots".into());
        }
    }
    Ok(())
}

impl Prepared {
    #[cfg(test)]
    pub fn stage(
        &mut self,
        profile: &ChildProfile,
        workspace: &Path,
        checkpoint: &serde_json::Value,
    ) -> Result<(), String> {
        self.stage_retained(profile, workspace, checkpoint, None)
    }

    pub fn stage_retained(
        &mut self,
        profile: &ChildProfile,
        workspace: &Path,
        checkpoint: &serde_json::Value,
        retained: Option<&tachyond::retained_storage::RetainedStorage>,
    ) -> Result<(), String> {
        use std::os::unix::fs::PermissionsExt;
        let err = |e: std::io::Error| e.to_string();
        let destination = workspace.parent().ok_or("missing managed destination")?;
        if destination.parent() != Some(profile.managed_root.as_path()) {
            return Err("invalid managed workspace destination".into());
        }
        let managed = open_root(&profile.managed_root)?;
        let metadata = managed.metadata().map_err(err)?;
        if metadata.uid() != nix::unistd::Uid::effective().as_raw() || metadata.mode() & 0o022 != 0
        {
            return Err("managed root must be host-owned and not group/other writable".into());
        }
        #[cfg(test)]
        self::checkpoint("root")?;
        let checkpoint = serde_json::json!({"schema_version": 1, "destination": destination, "descriptor": checkpoint, "profile": profile});
        let encoded = serde_json::to_vec(&checkpoint).map_err(|e| e.to_string())?;
        if encoded.len() > 262144 {
            return Err("snapshot descriptor too large".into());
        }
        let temp = PathBuf::from(format!(
            ".proposal-{}",
            destination
                .file_name()
                .ok_or("missing slot name")?
                .to_str()
                .ok_or("invalid slot name")?
        ));
        let quarantine = temp.with_file_name(format!(
            ".quarantine-{}",
            destination.file_name().unwrap().to_str().unwrap()
        ));
        let destination = Path::new(destination.file_name().ok_or("missing slot name")?);
        let campaign = match checkpoint["descriptor"]["campaign_id"].as_str() {
            Some(campaign) => campaign,
            None if retained.is_none() => "",
            None => return Err("missing host snapshot campaign".into()),
        };
        let identity = format!("{:x}", Sha256::digest(&encoded));
        let resource_id = |copy| format!("{}:{copy}", workspace.display());
        let reconcile = |directory: &File, copy| -> Result<(), String> {
            if let Some(ledger) = retained {
                let mut actual = (encoded.len() as u64)
                    .checked_mul(2)
                    .ok_or("snapshot size overflow")?;
                for (index, input) in profile.inputs.iter().enumerate() {
                    for file in &input.files {
                        actual = actual
                            .checked_add(
                                open_relative(
                                    directory,
                                    &PathBuf::from("work/inputs")
                                        .join(index.to_string())
                                        .join(&file.path),
                                )?
                                .metadata()
                                .map_err(err)?
                                .len(),
                            )
                            .ok_or("snapshot size overflow")?;
                    }
                }
                let id = resource_id(copy);
                let receipt = ledger
                    .get(campaign, "snapshot", &id)?
                    .ok_or("snapshot has no retained reservation")?;
                let plan: serde_json::Value =
                    serde_json::from_str(&receipt.identity).map_err(|e| e.to_string())?;
                if plan["descriptor"] != identity {
                    return Err("snapshot retained identity conflict".into());
                }
                ledger.ready(&receipt, actual)?;
            }
            Ok(())
        };
        if exists(&managed, destination)? {
            let directory = open_relative(&managed, destination)?;
            if verify(profile, &directory, &checkpoint, true).is_ok() {
                associated(&managed, destination, &directory)?;
                root_associated(&profile.managed_root, &managed)?;
                reconcile(
                    &directory,
                    if exists(&managed, &quarantine)? { 1 } else { 0 },
                )?;
                return Ok(());
            }
            verify(profile, &directory, &checkpoint, false)?;
            root_associated(&profile.managed_root, &managed)?;
            move_snapshot(&managed, destination, &quarantine, &directory)?;
            return Err("invalid prepared snapshot quarantined; operator review required".into());
        }
        // A fixed temporary name bounds retained preparation to one per slot.
        // One quarantine per slot is retained; further failures block reuse.
        if exists(&managed, &quarantine)? {
            return Err(
                "staging quarantine retained; operator review required before further preparation"
                    .into(),
            );
        }
        if exists(&managed, &temp)? {
            let directory = open_relative(&managed, &temp)?;
            if verify(profile, &directory, &checkpoint, true).is_ok() {
                root_associated(&profile.managed_root, &managed)?;
                move_snapshot(&managed, &temp, destination, &directory)?;
                root_associated(&profile.managed_root, &managed)?;
                reconcile(&directory, 0)?;
                return Ok(());
            }
            verify(profile, &directory, &checkpoint, false)?;
            let was_prepared = exists(&directory, Path::new("prepared.json"))?;
            root_associated(&profile.managed_root, &managed)?;
            move_snapshot(&managed, &temp, &quarantine, &directory)?;
            if was_prepared {
                return Err(
                    "tampered prepared snapshot quarantined; explicit operator review required"
                        .into(),
                );
            }
        }
        let copy = if exists(&managed, &quarantine)? { 1 } else { 0 };
        let receipt = if let Some(ledger) = retained {
            let id = resource_id(copy);
            let (expected, plan) = if let Some(old) = ledger.get(campaign, "snapshot", &id)? {
                if old.ready.is_some() {
                    return Err(
                        "reconciled snapshot is missing; refusing to reuse its charge".into(),
                    );
                }
                let plan: serde_json::Value =
                    serde_json::from_str(&old.identity).map_err(|e| e.to_string())?;
                if plan["descriptor"] != identity {
                    return Err("snapshot retained identity conflict".into());
                }
                (old.expected, old.identity)
            } else {
                let mut expected = (encoded.len() as u64)
                    .checked_mul(2)
                    .ok_or("snapshot size overflow")?;
                let mut input_bytes = 0u64;
                let mut files = Vec::new();
                for (index, input) in profile.inputs.iter().enumerate() {
                    let root = open_root(&input.root)?;
                    for file in &input.files {
                        let source = open_relative(&root, &file.path)?;
                        let metadata = source.metadata().map_err(err)?;
                        if !metadata.is_file() {
                            return Err("input is not a regular file".into());
                        }
                        input_bytes = input_bytes
                            .checked_add(metadata.len())
                            .ok_or("snapshot size overflow")?;
                        files.push(serde_json::json!({"source":index,"path":file.path,"size_bytes":metadata.len(),"sha256":file.sha256}));
                    }
                }
                if input_bytes > profile.max_input_bytes {
                    return Err("input byte quota exceeded".into());
                }
                expected = expected
                    .checked_add(input_bytes)
                    .ok_or("snapshot size overflow")?;
                (
                    expected,
                    serde_json::to_string(
                        &serde_json::json!({"descriptor":identity,"files":files}),
                    )
                    .map_err(|e| e.to_string())?,
                )
            };
            Some(ledger.reserve(campaign, "snapshot", &id, expected, &plan)?)
        } else {
            None
        };
        let staging = create_directory(&managed, &temp)?;
        #[cfg(test)]
        self::checkpoint("mkdir")?;
        let mut owner = create_file(
            &staging,
            Path::new("owned.json"),
            Mode::from_raw_mode(0o400),
        )?;
        owner.write_all(&encoded).map_err(err)?;
        owner.sync_all().map_err(err)?;
        staging.sync_all().map_err(err)?;
        managed.sync_all().map_err(err)?;
        #[cfg(test)]
        self::checkpoint("owned")?;
        let work = create_directory(&staging, Path::new("work"))?;
        let home = create_directory(&staging, Path::new("home"))?;
        let mut directories = std::collections::BTreeMap::from([
            (PathBuf::from("work"), work),
            (PathBuf::from("home"), home),
        ]);
        let mut bytes = 0u64;
        let mut files = 0usize;
        for (index, input) in profile.inputs.iter().enumerate() {
            let root = open_root(&input.root)?;
            #[cfg(test)]
            self::checkpoint("source")?;
            for approved in &input.files {
                files += 1;
                if files > profile.max_input_files {
                    return Err("input file quota exceeded".into());
                }
                let remaining = profile
                    .max_input_bytes
                    .checked_sub(bytes)
                    .ok_or("input byte quota exceeded")?;
                let data = read_regular(&root, &approved.path, remaining)?;
                if let Some(receipt) = &receipt {
                    let plan: serde_json::Value =
                        serde_json::from_str(&receipt.identity).map_err(|e| e.to_string())?;
                    if let Some(files) = plan["files"].as_array() {
                        let expected = files
                            .iter()
                            .find(|f| {
                                f["source"] == index
                                    && f["path"] == approved.path.to_string_lossy().as_ref()
                            })
                            .ok_or("snapshot input missing from immutable plan")?;
                        if expected["size_bytes"].as_u64() != Some(data.len() as u64) {
                            return Err("snapshot input size changed".into());
                        }
                    }
                }
                bytes += data.len() as u64;
                if receipt.as_ref().is_some_and(|r| {
                    bytes
                        .checked_add(2 * encoded.len() as u64)
                        .is_none_or(|n| n > r.expected)
                }) {
                    return Err("snapshot source grew beyond retained reservation".into());
                }
                if bytes > profile.max_input_bytes
                    || format!("{:x}", Sha256::digest(&data)) != approved.sha256
                {
                    return Err("input quota or immutable SHA-256 version mismatch".into());
                }
                let target = PathBuf::from("work")
                    .join("inputs")
                    .join(index.to_string())
                    .join(&approved.path);
                let mut parent = staging.try_clone().map_err(err)?;
                let mut relative = PathBuf::new();
                for part in target.parent().unwrap().components() {
                    let Component::Normal(name) = part else {
                        return Err("invalid input path".into());
                    };
                    relative.push(name);
                    parent = if let Some(pinned) = directories.get(&relative) {
                        pinned.try_clone().map_err(err)?
                    } else {
                        let created = create_directory(&parent, Path::new(name))?;
                        directories.insert(relative.clone(), created.try_clone().map_err(err)?);
                        created
                    };
                }
                #[cfg(test)]
                self::checkpoint("write")?;
                let mut file = create_file(
                    &parent,
                    Path::new(target.file_name().unwrap()),
                    Mode::from_raw_mode(0o600),
                )?;
                #[cfg(test)]
                self::checkpoint("copy")?;
                file.write_all(&data).map_err(err)?;
                file.set_permissions(std::fs::Permissions::from_mode(0o444))
                    .map_err(err)?;
                file.sync_all().map_err(err)?;
            }
        }
        // Persist child entries before publishing the durable completion marker.
        for (path, directory) in directories.iter().rev() {
            directory.sync_all().map_err(err)?;
            associated(&staging, path, directory)?;
        }
        let mut record = create_file(
            &staging,
            Path::new("prepared.json"),
            Mode::from_raw_mode(0o400),
        )?;
        record.write_all(&encoded).map_err(err)?;
        record.sync_all().map_err(err)?;
        staging.sync_all().map_err(err)?;
        #[cfg(test)]
        self::checkpoint("prepared")?;
        verify(profile, &staging, &checkpoint, true)?;
        root_associated(&profile.managed_root, &managed)?;
        move_snapshot(&managed, &temp, destination, &staging)?;
        root_associated(&profile.managed_root, &managed)?;
        reconcile(&staging, copy)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::campaign::{ChildInput, InputFile};

    #[test]
    fn retained_snapshot_quarantine_requires_an_independent_reservation() {
        for allow_second in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (profile, workspace, mut checkpoint) = fixture(root.path());
            checkpoint["campaign_id"] = serde_json::json!("campaign");
            let ledger = tachyond::retained_storage::RetainedStorage::new(
                std::sync::Arc::new(
                    redb::Database::create(root.path().join("runtime.redb")).unwrap(),
                ),
                1024 * 1024,
            )
            .unwrap();
            CRASH.with(|c| c.set(Some("owned")));
            assert!(Prepared
                .stage_retained(&profile, &workspace, &checkpoint, Some(&ledger))
                .is_err());
            let id = format!("{}:0", workspace.display());
            let first = ledger.get("campaign", "snapshot", &id).unwrap().unwrap();
            assert!(first.ready.is_none());
            ledger
                .configure(
                    "campaign",
                    Some(first.expected * if allow_second { 2 } else { 1 }),
                )
                .unwrap();
            let result = Prepared.stage_retained(&profile, &workspace, &checkpoint, Some(&ledger));
            assert_eq!(result.is_ok(), allow_second);
            assert!(profile.managed_root.join(".quarantine-slot").exists());
            assert_eq!(
                ledger.summary("campaign").unwrap()["campaign_charged_bytes"],
                first.expected * if allow_second { 2 } else { 1 }
            );
            if allow_second {
                let second = ledger
                    .get(
                        "campaign",
                        "snapshot",
                        &format!("{}:1", workspace.display()),
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(second.ready, Some(second.expected));
                std::fs::write(profile.inputs[0].root.join("input"), b"changed").unwrap();
                Prepared
                    .stage_retained(&profile, &workspace, &checkpoint, Some(&ledger))
                    .unwrap();
                assert_eq!(
                    ledger.summary("campaign").unwrap()["campaign_charged_bytes"],
                    first.expected * 2
                );
            }
        }
    }

    #[test]
    fn retained_snapshot_source_mutation_cannot_be_ready_or_free() {
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, mut checkpoint) = fixture(root.path());
        checkpoint["campaign_id"] = serde_json::json!("campaign");
        let ledger = tachyond::retained_storage::RetainedStorage::new(
            std::sync::Arc::new(redb::Database::create(root.path().join("runtime.redb")).unwrap()),
            1024 * 1024,
        )
        .unwrap();
        let source = profile.inputs[0].root.join("input");
        HOOK.with(|hook| {
            *hook.borrow_mut() = Some((
                "source",
                Box::new(move || {
                    std::fs::write(source, b"modified").unwrap();
                }),
            ))
        });
        assert!(Prepared
            .stage_retained(&profile, &workspace, &checkpoint, Some(&ledger))
            .is_err());
        let receipt = ledger
            .get(
                "campaign",
                "snapshot",
                &format!("{}:0", workspace.display()),
            )
            .unwrap()
            .unwrap();
        assert!(receipt.ready.is_none());
        ledger
            .configure("campaign", Some(receipt.expected))
            .unwrap();
        assert!(ledger
            .reserve("campaign", "trace", "next", 1, "hash")
            .is_err());
        assert!(!workspace.exists());
    }

    #[test]
    fn retained_ready_snapshot_missing_directory_cannot_recopy() {
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, mut checkpoint) = fixture(root.path());
        checkpoint["campaign_id"] = serde_json::json!("campaign");
        let ledger = tachyond::retained_storage::RetainedStorage::new(
            std::sync::Arc::new(redb::Database::create(root.path().join("runtime.redb")).unwrap()),
            1024 * 1024,
        )
        .unwrap();
        Prepared
            .stage_retained(&profile, &workspace, &checkpoint, Some(&ledger))
            .unwrap();
        let before = ledger.summary("campaign").unwrap();
        std::fs::rename(workspace.parent().unwrap(), root.path().join("detached")).unwrap();
        assert!(Prepared
            .stage_retained(&profile, &workspace, &checkpoint, Some(&ledger))
            .is_err());
        assert!(!profile.managed_root.join(".proposal-slot").exists());
        assert_eq!(ledger.summary("campaign").unwrap(), before);
    }

    fn fixture(root: &Path) -> (ChildProfile, PathBuf, serde_json::Value) {
        let managed_root = root.join("managed");
        let source = root.join("source");
        std::fs::create_dir(&managed_root).unwrap();
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("input"), b"approved").unwrap();
        let profile = ChildProfile {
            profile_ids: vec![],
            evaluator: None,
            profile_id: "test".into(), max_proposals: 1, max_objective_bytes: 100,
            max_context_refs: 0, managed_root: managed_root.clone(),
            inputs: vec![ChildInput { root: source, files: vec![InputFile {
                path: "input".into(), sha256: format!("{:x}", Sha256::digest(b"approved")),
            }] }], max_input_files: 1, max_input_bytes: 8,
            permissions: serde_json::from_value(serde_json::json!({"task_type":"coding_read_only", "allow_exec":false, "allow_python":false})).unwrap(),
            work_tokens: 1, work_cost_micro_usd: 1, verification_tokens: 1, verification_cost_micro_usd: 1,
        };
        (
            profile,
            managed_root.join("slot/work"),
            serde_json::json!({"campaign":"campaign", "proposal":"exact"}),
        )
    }

    #[test]
    fn root_acquisition_rejects_symlink_ancestors() {
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, checkpoint) = fixture(root.path());
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(root.path(), &alias).unwrap();
        assert!(open_root(&alias.join("managed")).is_err());
        let mut changed = profile.clone();
        changed.managed_root = alias.join("managed");
        assert!(Prepared
            .stage(
                &changed,
                &changed.managed_root.join("slot/work"),
                &checkpoint
            )
            .is_err());
        changed = profile;
        changed.inputs[0].root = alias.join("source");
        assert!(Prepared.stage(&changed, &workspace, &checkpoint).is_err());
        assert!(!workspace.exists());
    }

    #[test]
    fn ancestor_swaps_never_redirect_writes_verification_or_quarantine() {
        for point in ["root", "mkdir", "write", "verify", "rename"] {
            let root = tempfile::tempdir().unwrap();
            let (mut profile, _, checkpoint) = fixture(root.path());
            let ancestor = root.path().join("ancestor");
            std::fs::create_dir(&ancestor).unwrap();
            std::fs::rename(&profile.managed_root, ancestor.join("managed")).unwrap();
            profile.managed_root = ancestor.join("managed");
            let workspace = profile.managed_root.join("slot/work");
            if ["verify", "rename"].contains(&point) {
                Prepared.stage(&profile, &workspace, &checkpoint).unwrap();
                std::fs::remove_file(workspace.join("inputs/0/input")).unwrap();
                std::fs::write(workspace.join("inputs/0/input"), b"tampered").unwrap();
            }
            let outside = tempfile::tempdir().unwrap();
            for name in ["slot", ".proposal-slot", ".quarantine-slot"] {
                std::fs::create_dir_all(
                    outside
                        .path()
                        .join("managed")
                        .join(name)
                        .join("work/inputs/0"),
                )
                .unwrap();
                std::fs::write(
                    outside.path().join("managed").join(name).join("owned.json"),
                    b"sentinel",
                )
                .unwrap();
                std::fs::write(
                    outside
                        .path()
                        .join("managed")
                        .join(name)
                        .join("work/inputs/0/input"),
                    b"sentinel",
                )
                .unwrap();
            }
            let detached = root.path().join("detached");
            let target = outside.path().to_owned();
            HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    point,
                    Box::new(move || {
                        std::fs::rename(&ancestor, detached).unwrap();
                        std::os::unix::fs::symlink(target, ancestor).unwrap();
                    }),
                ))
            });
            assert!(
                Prepared.stage(&profile, &workspace, &checkpoint).is_err(),
                "{point}"
            );
            HOOK.with(|hook| assert!(hook.borrow().is_none(), "{point}"));
            for name in ["slot", ".proposal-slot", ".quarantine-slot"] {
                let directory = outside.path().join("managed").join(name);
                assert_eq!(
                    std::fs::read(directory.join("owned.json")).unwrap(),
                    b"sentinel",
                    "{point}"
                );
                assert_eq!(
                    std::fs::read(directory.join("work/inputs/0/input")).unwrap(),
                    b"sentinel",
                    "{point}"
                );
                assert!(!directory.join("prepared.json").exists());
            }
        }
    }

    #[test]
    fn intermediate_source_and_target_symlink_swaps_fail_closed() {
        for point in ["source", "write", "verify"] {
            let root = tempfile::tempdir().unwrap();
            let (mut profile, workspace, checkpoint) = fixture(root.path());
            std::fs::create_dir(profile.inputs[0].root.join("nested")).unwrap();
            std::fs::rename(
                profile.inputs[0].root.join("input"),
                profile.inputs[0].root.join("nested/input"),
            )
            .unwrap();
            profile.inputs[0].files[0].path = "nested/input".into();
            if point == "verify" {
                Prepared.stage(&profile, &workspace, &checkpoint).unwrap();
            }
            let replaced = match point {
                "source" => profile.inputs[0].root.join("nested"),
                "write" => profile
                    .managed_root
                    .join(".proposal-slot/work/inputs/0/nested"),
                _ => workspace.join("inputs/0/nested"),
            };
            let outside = tempfile::tempdir().unwrap();
            std::fs::write(outside.path().join("input"), b"sentinel").unwrap();
            let target = outside.path().to_owned();
            HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    point,
                    Box::new(move || {
                        std::fs::rename(&replaced, replaced.with_file_name("detached")).unwrap();
                        std::os::unix::fs::symlink(target, replaced).unwrap();
                    }),
                ))
            });
            assert!(
                Prepared.stage(&profile, &workspace, &checkpoint).is_err(),
                "{point}"
            );
            assert_eq!(
                std::fs::read(outside.path().join("input")).unwrap(),
                b"sentinel"
            );
            assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 1);
            assert!(!profile.managed_root.join(".quarantine-slot").exists());
        }
    }

    #[test]
    fn rename_substitution_is_retained_and_never_acknowledged_or_deleted() {
        for quarantine in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (profile, workspace, checkpoint) = fixture(root.path());
            if quarantine {
                Prepared.stage(&profile, &workspace, &checkpoint).unwrap();
                std::fs::remove_file(workspace.join("inputs/0/input")).unwrap();
                std::fs::write(workspace.join("inputs/0/input"), b"tampered").unwrap();
            }
            let source =
                profile
                    .managed_root
                    .join(if quarantine { "slot" } else { ".proposal-slot" });
            HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    "rename",
                    Box::new(move || {
                        std::fs::rename(&source, source.with_file_name("pinned-original")).unwrap();
                        std::fs::create_dir(&source).unwrap();
                        std::fs::write(source.join("unrelated"), b"keep").unwrap();
                    }),
                ))
            });
            let error = Prepared
                .stage(&profile, &workspace, &checkpoint)
                .unwrap_err();
            assert!(error.contains("rename identity mismatch"), "{error}");
            let moved = profile.managed_root.join(if quarantine {
                ".quarantine-slot"
            } else {
                "slot"
            });
            assert_eq!(std::fs::read(moved.join("unrelated")).unwrap(), b"keep");
            assert!(profile
                .managed_root
                .join("pinned-original/owned.json")
                .is_file());
            assert!(Prepared.stage(&profile, &workspace, &checkpoint).is_err());
            assert_eq!(std::fs::read(moved.join("unrelated")).unwrap(), b"keep");
        }
    }

    #[test]
    fn replaced_verified_directory_is_not_reused_or_quarantined() {
        for corrupt in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (profile, workspace, checkpoint) = fixture(root.path());
            Prepared.stage(&profile, &workspace, &checkpoint).unwrap();
            if corrupt {
                std::fs::remove_file(workspace.join("inputs/0/input")).unwrap();
                std::fs::write(workspace.join("inputs/0/input"), b"tampered").unwrap();
            }
            let source = workspace.parent().unwrap().to_owned();
            HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    "verify",
                    Box::new(move || {
                        std::fs::rename(&source, source.with_file_name("original")).unwrap();
                        std::fs::create_dir(&source).unwrap();
                        std::fs::write(source.join("unrelated"), b"keep").unwrap();
                    }),
                ))
            });
            assert!(Prepared.stage(&profile, &workspace, &checkpoint).is_err());
            assert_eq!(
                std::fs::read(workspace.parent().unwrap().join("unrelated")).unwrap(),
                b"keep"
            );
            assert!(!profile.managed_root.join(".quarantine-slot").exists());
        }
    }

    #[test]
    fn pinned_source_root_does_not_follow_replacement() {
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, checkpoint) = fixture(root.path());
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("input"), b"sentinel").unwrap();
        let source = profile.inputs[0].root.clone();
        let target = outside.path().to_owned();
        HOOK.with(|hook| {
            *hook.borrow_mut() = Some((
                "source",
                Box::new(move || {
                    std::fs::rename(&source, source.with_file_name("original")).unwrap();
                    std::os::unix::fs::symlink(target, source).unwrap();
                }),
            ))
        });
        Prepared.stage(&profile, &workspace, &checkpoint).unwrap();
        assert_eq!(
            std::fs::read(workspace.join("inputs/0/input")).unwrap(),
            b"approved"
        );
        assert_eq!(
            std::fs::read(outside.path().join("input")).unwrap(),
            b"sentinel"
        );
    }

    #[test]
    fn publication_and_quarantine_never_replace_existing_entries() {
        for quarantine in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (profile, workspace, checkpoint) = fixture(root.path());
            if quarantine {
                Prepared.stage(&profile, &workspace, &checkpoint).unwrap();
                std::fs::remove_file(workspace.join("inputs/0/input")).unwrap();
                std::fs::write(workspace.join("inputs/0/input"), b"tampered").unwrap();
            }
            let target = profile.managed_root.join(if quarantine {
                ".quarantine-slot"
            } else {
                "slot"
            });
            let occupied = target.clone();
            HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    "rename",
                    Box::new(move || {
                        std::fs::create_dir(&occupied).unwrap();
                        std::fs::write(occupied.join("unrelated"), b"keep").unwrap();
                    }),
                ))
            });
            assert!(Prepared.stage(&profile, &workspace, &checkpoint).is_err());
            assert_eq!(std::fs::read(target.join("unrelated")).unwrap(), b"keep");
            let source =
                profile
                    .managed_root
                    .join(if quarantine { "slot" } else { ".proposal-slot" });
            assert!(source.join("owned.json").is_file());
        }
    }

    #[test]
    fn nested_inputs_recover_after_prepared_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let (mut profile, workspace, checkpoint) = fixture(root.path());
        std::fs::create_dir_all(profile.inputs[0].root.join("nested/deep")).unwrap();
        std::fs::rename(
            profile.inputs[0].root.join("input"),
            profile.inputs[0].root.join("nested/deep/input"),
        )
        .unwrap();
        profile.inputs[0].files[0].path = "nested/deep/input".into();
        CRASH.with(|point| point.set(Some("prepared")));
        assert!(Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap_err()
            .contains("injected crash"));
        std::fs::remove_file(profile.inputs[0].root.join("nested/deep/input")).unwrap();
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        assert_eq!(
            std::fs::read(workspace.join("inputs/0/nested/deep/input")).unwrap(),
            b"approved"
        );
    }

    #[test]
    fn prepared_recovery_uses_snapshot_not_changed_source() {
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, checkpoint) = fixture(root.path());
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        std::fs::write(profile.inputs[0].root.join("input"), b"changed!").unwrap();
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        let temp = profile.managed_root.join(".proposal-slot");
        std::fs::rename(workspace.parent().unwrap(), &temp).unwrap();
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        assert_eq!(
            std::fs::read(workspace.join("inputs/0/input")).unwrap(),
            b"approved"
        );
        assert!(Prepared::default()
            .stage(
                &profile,
                &workspace,
                &serde_json::json!({"proposal":"changed"})
            )
            .is_err());
    }

    #[test]
    fn interrupted_copy_quarantines_once_and_does_not_expand_quota() {
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, checkpoint) = fixture(root.path());
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        let temp = profile.managed_root.join(".proposal-slot");
        std::fs::rename(workspace.parent().unwrap(), &temp).unwrap();
        std::fs::remove_file(temp.join("prepared.json")).unwrap();
        std::fs::remove_file(temp.join("work/inputs/0/input")).unwrap();
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        assert!(profile
            .managed_root
            .join(".quarantine-slot/owned.json")
            .is_file());
        std::fs::rename(workspace.parent().unwrap(), &temp).unwrap();
        std::fs::remove_file(temp.join("prepared.json")).unwrap();
        assert!(Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .is_err());
        assert!(temp.exists());
    }

    #[test]
    fn markerless_symlink_and_unexpected_files_are_untouched() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, checkpoint) = fixture(root.path());
        let temp = profile.managed_root.join(".proposal-slot");
        std::fs::create_dir(&temp).unwrap();
        assert!(Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .is_err());
        assert!(temp.is_dir());
        symlink(
            profile.inputs[0].root.join("input"),
            temp.join("owned.json"),
        )
        .unwrap();
        assert!(Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .is_err());
        assert!(std::fs::symlink_metadata(temp.join("owned.json"))
            .unwrap()
            .file_type()
            .is_symlink());
        std::fs::remove_file(temp.join("owned.json")).unwrap();
        std::fs::remove_dir(&temp).unwrap();
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        std::fs::write(workspace.join("user-file"), b"keep").unwrap();
        assert!(Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .is_err());
        assert_eq!(std::fs::read(workspace.join("user-file")).unwrap(), b"keep");
    }

    #[test]
    fn wrong_entry_types_are_retained_without_promotion_or_quarantine() {
        for staged in [false, true] {
            for relative in ["home", "work/inputs/0/input"] {
                let root = tempfile::tempdir().unwrap();
                let (profile, workspace, checkpoint) = fixture(root.path());
                Prepared::default()
                    .stage(&profile, &workspace, &checkpoint)
                    .unwrap();
                let destination = workspace.parent().unwrap();
                let temp = profile.managed_root.join(".proposal-slot");
                let directory = if staged {
                    std::fs::rename(destination, &temp).unwrap();
                    temp.as_path()
                } else {
                    destination
                };
                let path = directory.join(relative);
                if relative == "home" {
                    std::fs::remove_dir(&path).unwrap();
                    std::fs::write(&path, b"user data").unwrap();
                } else {
                    std::fs::remove_file(&path).unwrap();
                    std::fs::create_dir(&path).unwrap();
                }
                assert!(Prepared::default()
                    .stage(&profile, &workspace, &checkpoint)
                    .is_err());
                assert!(path.exists());
                assert!(!profile.managed_root.join(".quarantine-slot").exists());
                if staged {
                    assert!(!destination.exists());
                }
                if relative == "home" {
                    assert_eq!(std::fs::read(&path).unwrap(), b"user data");
                }
            }
        }
    }

    #[test]
    fn tampered_prepared_snapshot_is_quarantined_not_replaced() {
        let root = tempfile::tempdir().unwrap();
        let (profile, workspace, checkpoint) = fixture(root.path());
        Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .unwrap();
        let input = workspace.join("inputs/0/input");
        std::fs::remove_file(&input).unwrap();
        std::fs::write(&input, b"tampered").unwrap();
        assert!(Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .is_err());
        assert!(!workspace.exists());
        assert_eq!(
            std::fs::read(
                profile
                    .managed_root
                    .join(".quarantine-slot/work/inputs/0/input")
            )
            .unwrap(),
            b"tampered"
        );
        assert!(Prepared::default()
            .stage(&profile, &workspace, &checkpoint)
            .is_err());
        assert!(!workspace.exists());
    }
}
